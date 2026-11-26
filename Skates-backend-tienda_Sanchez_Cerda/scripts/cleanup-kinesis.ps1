<#
.SYNOPSIS
Elimina manualmente los recursos Kinesis de Skates para detener costos de pruebas.

.DESCRIPTION
Elimina solo los streams configurados, la alarma IteratorAge y las tablas DynamoDB
de leases KCL para analytics y notifications. No elimina SNS, SQS, MySQL ni otros
recursos. Si los streams pertenecen a CloudFormation, se puede indicar el nombre
del stack dedicado; se aborta si contiene tipos de recursos ajenos a Kinesis.

Detén primero las instancias backend con KINESIS_ENABLED=true y los consumidores KCL.
#>
[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = "High")]
param(
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$Region,

    [string]$Profile,
    [string]$CloudFormationStackName,
    [string]$OrdersStreamName,
    [string]$InventoryStreamName,
    [string]$DeadLetterStreamName,
    [string]$AnalyticsApplicationName,
    [string]$NotificationsApplicationName
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($OrdersStreamName)) {
    $OrdersStreamName = if ($env:KINESIS_ORDERS_STREAM_NAME) { $env:KINESIS_ORDERS_STREAM_NAME } else { "skates-orders-events" }
}
if ([string]::IsNullOrWhiteSpace($InventoryStreamName)) {
    $InventoryStreamName = if ($env:KINESIS_INVENTORY_STREAM_NAME) { $env:KINESIS_INVENTORY_STREAM_NAME } else { "skates-inventory-events" }
}
if ([string]::IsNullOrWhiteSpace($DeadLetterStreamName)) {
    $DeadLetterStreamName = if ($env:KINESIS_DLQ_STREAM_NAME) { $env:KINESIS_DLQ_STREAM_NAME } else { "skates-dead-letter-stream" }
}
if ([string]::IsNullOrWhiteSpace($AnalyticsApplicationName)) {
    $AnalyticsApplicationName = if ($env:KINESIS_ANALYTICS_APP_NAME) { $env:KINESIS_ANALYTICS_APP_NAME } else { "skates-analytics-app" }
}
if ([string]::IsNullOrWhiteSpace($NotificationsApplicationName)) {
    $NotificationsApplicationName = if ($env:KINESIS_NOTIFICATIONS_APP_NAME) { $env:KINESIS_NOTIFICATIONS_APP_NAME } else { "skates-notifications-app" }
}

$aws = Get-Command aws -ErrorAction SilentlyContinue
if (-not $aws) {
    throw "AWS CLI no está disponible en PATH. No se eliminó ningún recurso."
}

function Invoke-AwsCli {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments,

        [switch]$AllowMissing
    )

    $allArguments = @($Arguments) + @("--region", $Region, "--output", "json")
    if (-not [string]::IsNullOrWhiteSpace($Profile)) {
        $allArguments += @("--profile", $Profile)
    }

    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $output = & $aws.Source @allArguments 2>&1
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    $text = ($output | Out-String).Trim()
    if ($exitCode -ne 0) {
        if ($AllowMissing -and $text -match "ResourceNotFound|ResourceNotFoundException|does not exist|not found") {
            return $null
        }
        throw "Falló AWS CLI ($($Arguments -join ' ')): $text"
    }
    return $text
}

$identityJson = Invoke-AwsCli -Arguments @("sts", "get-caller-identity")
$identity = $identityJson | ConvertFrom-Json

Write-Host ""
Write-Host "Cuenta AWS: $($identity.Account)"
Write-Host "Identidad:  $($identity.Arn)"
Write-Host "Región:     $Region"
Write-Host ""
Write-Host "Se eliminarán únicamente estos recursos Kinesis:"
Write-Host "  Streams: $OrdersStreamName, $InventoryStreamName, $DeadLetterStreamName"
Write-Host "  Alarma:  $OrdersStreamName-iterator-age"
Write-Host "  Tablas KCL: $AnalyticsApplicationName, $NotificationsApplicationName"
if (-not [string]::IsNullOrWhiteSpace($CloudFormationStackName)) {
    Write-Host "  Stack dedicado: $CloudFormationStackName"
}
Write-Host "No se eliminarán SNS, SQS, MySQL ni recursos de otros nombres."

if (-not $WhatIfPreference) {
    $confirmation = Read-Host 'Para continuar, escribe exactamente ELIMINAR KINESIS'
    if ($confirmation -cne "ELIMINAR KINESIS") {
        throw "Confirmación incorrecta. No se eliminó ningún recurso."
    }
}

if (-not [string]::IsNullOrWhiteSpace($CloudFormationStackName)) {
    $stackJson = Invoke-AwsCli -Arguments @(
        "cloudformation", "describe-stacks", "--stack-name", $CloudFormationStackName
    )
    $stack = $stackJson | ConvertFrom-Json
    $stackStatus = $stack.Stacks[0].StackStatus
    if ($stackStatus -like "DELETE_*") {
        throw "El stack $CloudFormationStackName ya está en proceso de eliminación ($stackStatus)."
    }

    $resourcesJson = Invoke-AwsCli -Arguments @(
        "cloudformation", "list-stack-resources", "--stack-name", $CloudFormationStackName
    )
    $resources = ($resourcesJson | ConvertFrom-Json).StackResourceSummaries
    $allowedTypes = @("AWS::Kinesis::Stream", "AWS::CloudWatch::Alarm")
    $unexpectedTypes = @($resources | Where-Object {
        $_.ResourceStatus -ne "DELETE_COMPLETE" -and $allowedTypes -notcontains $_.ResourceType
    })
    if ($unexpectedTypes.Count -gt 0) {
        $typeList = ($unexpectedTypes | ForEach-Object {
            "$($_.LogicalResourceId) [$($_.ResourceType)]"
        }) -join ", "
        throw "El stack contiene recursos ajenos a Kinesis: $typeList. No se eliminó el stack."
    }

    if ($PSCmdlet.ShouldProcess("CloudFormation $CloudFormationStackName", "Eliminar stack dedicado Kinesis")) {
        Invoke-AwsCli -Arguments @(
            "cloudformation", "delete-stack", "--stack-name", $CloudFormationStackName
        ) | Out-Null
        Invoke-AwsCli -Arguments @(
            "cloudformation", "wait", "stack-delete-complete", "--stack-name", $CloudFormationStackName
        ) | Out-Null
    }
}

foreach ($streamName in @($OrdersStreamName, $InventoryStreamName, $DeadLetterStreamName)) {
    $streamJson = Invoke-AwsCli -Arguments @(
        "kinesis", "describe-stream-summary", "--stream-name", $streamName
    ) -AllowMissing
    if ($null -eq $streamJson) {
        Write-Host "Stream ausente; sin acción: $streamName"
        continue
    }

    if ($PSCmdlet.ShouldProcess("Kinesis stream $streamName", "Eliminar todos sus shards y consumidores registrados")) {
        Invoke-AwsCli -Arguments @(
            "kinesis", "delete-stream", "--stream-name", $streamName, "--enforce-consumer-deletion"
        ) | Out-Null
        Invoke-AwsCli -Arguments @(
            "kinesis", "wait", "stream-not-exists", "--stream-name", $streamName
        ) | Out-Null
    }
}

$alarmName = "$OrdersStreamName-iterator-age"
if ($PSCmdlet.ShouldProcess("CloudWatch alarm $alarmName", "Eliminar alarma de retraso Kinesis")) {
    Invoke-AwsCli -Arguments @("cloudwatch", "delete-alarms", "--alarm-names", $alarmName) | Out-Null
}

foreach ($tableName in @($AnalyticsApplicationName, $NotificationsApplicationName)) {
    $tableJson = Invoke-AwsCli -Arguments @(
        "dynamodb", "describe-table", "--table-name", $tableName
    ) -AllowMissing
    if ($null -eq $tableJson) {
        Write-Host "Tabla KCL ausente; sin acción: $tableName"
        continue
    }

    if ($PSCmdlet.ShouldProcess("DynamoDB KCL table $tableName", "Eliminar leases y checkpoints")) {
        Invoke-AwsCli -Arguments @("dynamodb", "delete-table", "--table-name", $tableName) | Out-Null
        Invoke-AwsCli -Arguments @(
            "dynamodb", "wait", "table-not-exists", "--table-name", $tableName
        ) | Out-Null
    }
}

Write-Host ""
Write-Host "Limpieza Kinesis completada. Verifica los recursos en la misma cuenta y región."
