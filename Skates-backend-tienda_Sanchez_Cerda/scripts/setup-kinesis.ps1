<#
.SYNOPSIS
Crea los streams Kinesis de Skates con sus cantidades de shards configuradas.

.DESCRIPTION
Crea los streams de ordenes, inventario y dead-letter si no existen, configura
su retencion al crearlos y crea la alarma de antiguedad de lectura. Los streams
existentes no se modifican; si el numero de shards no coincide, el script aborta.
Las tablas de leases KCL se crean automaticamente cuando arranca cada consumidor.
Si no se indica region o perfil, usa la configuracion existente de AWS CLI.
Se puede indicar un stack dedicado con CloudFormationStackName para desplegar
la plantilla infra/aws-skates-kinesis.yaml.
#>
[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = "Medium")]
param(
    [string]$Region,

    [string]$Profile,
    [string]$CloudFormationStackName,
    [string]$OrdersStreamName,
    [string]$InventoryStreamName,
    [string]$DeadLetterStreamName,
    [ValidateRange(1, 10000)]
    [int]$OrdersShardCount = 3,
    [ValidateRange(1, 10000)]
    [int]$InventoryShardCount = 3,
    [ValidateRange(1, 10000)]
    [int]$DeadLetterShardCount = 1,
    [ValidateRange(24, 8760)]
    [int]$OrdersRetentionHours = 24,
    [ValidateRange(24, 8760)]
    [int]$InventoryRetentionHours = 168,
    [ValidateRange(24, 8760)]
    [int]$DeadLetterRetentionHours = 168,
    [ValidateRange(1, 2147483647)]
    [int]$IteratorAgeThresholdMilliseconds = 60000,
    [ValidateRange(1, 2147483647)]
    [int]$AlarmEvaluationPeriods = 2,
    [ValidateRange(60, 86400)]
    [int]$AlarmPeriodSeconds = 60
)

$ErrorActionPreference = "Stop"

if ($AlarmPeriodSeconds % 60 -ne 0) {
    throw "AlarmPeriodSeconds debe ser multiplo de 60."
}

if ([string]::IsNullOrWhiteSpace($OrdersStreamName)) {
    $OrdersStreamName = if ($env:KINESIS_ORDERS_STREAM_NAME) { $env:KINESIS_ORDERS_STREAM_NAME } else { "skates-orders-events" }
}
if ([string]::IsNullOrWhiteSpace($InventoryStreamName)) {
    $InventoryStreamName = if ($env:KINESIS_INVENTORY_STREAM_NAME) { $env:KINESIS_INVENTORY_STREAM_NAME } else { "skates-inventory-events" }
}
if ([string]::IsNullOrWhiteSpace($DeadLetterStreamName)) {
    $DeadLetterStreamName = if ($env:KINESIS_DLQ_STREAM_NAME) { $env:KINESIS_DLQ_STREAM_NAME } else { "skates-dead-letter-stream" }
}
if ([string]::IsNullOrWhiteSpace($CloudFormationStackName)) {
    $CloudFormationStackName = $env:KINESIS_CLOUDFORMATION_STACK_NAME
}

$aws = Get-Command aws -ErrorAction SilentlyContinue
if (-not $aws) {
    throw "AWS CLI no esta disponible en PATH. No se creo ningun recurso."
}

function Invoke-AwsCli {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments,

        [switch]$AllowMissing
    )

    $allArguments = @($Arguments)
    if (-not [string]::IsNullOrWhiteSpace($Region)) {
        $allArguments += @("--region", $Region)
    }
    $allArguments += @("--output", "json")
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
        throw "Fallo AWS CLI ($($Arguments -join ' ')): $text"
    }
    return $text
}

if ([string]::IsNullOrWhiteSpace($Region)) {
    if (-not [string]::IsNullOrWhiteSpace($env:AWS_REGION)) {
        $Region = $env:AWS_REGION
    }
    elseif (-not [string]::IsNullOrWhiteSpace($env:AWS_DEFAULT_REGION)) {
        $Region = $env:AWS_DEFAULT_REGION
    }
    else {
        $regionArguments = @("configure", "get", "region")
        $configuredProfile = if (-not [string]::IsNullOrWhiteSpace($Profile)) {
            $Profile
        } else {
            $env:AWS_PROFILE
        }
        if (-not [string]::IsNullOrWhiteSpace($configuredProfile)) {
            $regionArguments += @("--profile", $configuredProfile)
        }
        $regionOutput = & $aws.Source @regionArguments 2>&1
        if ($LASTEXITCODE -ne 0) {
            throw "No se pudo leer la region configurada en AWS CLI: $(($regionOutput | Out-String).Trim())"
        }
        $Region = ($regionOutput | Out-String).Trim()
    }
    if ([string]::IsNullOrWhiteSpace($Region)) {
        throw "No hay una region configurada en AWS CLI. Configurala con 'aws configure' o indica -Region."
    }
}

$identityJson = Invoke-AwsCli -Arguments @("sts", "get-caller-identity")
$identity = $identityJson | ConvertFrom-Json

$streams = @(
    [pscustomobject]@{
        Name = $OrdersStreamName
        ShardCount = $OrdersShardCount
        RetentionHours = $OrdersRetentionHours
    },
    [pscustomobject]@{
        Name = $InventoryStreamName
        ShardCount = $InventoryShardCount
        RetentionHours = $InventoryRetentionHours
    },
    [pscustomobject]@{
        Name = $DeadLetterStreamName
        ShardCount = $DeadLetterShardCount
        RetentionHours = $DeadLetterRetentionHours
    }
)

if (($streams.Name | Select-Object -Unique).Count -ne $streams.Count) {
    throw "Los nombres de los tres streams deben ser distintos."
}

Write-Host ""
Write-Host "Cuenta AWS: $($identity.Account)"
Write-Host "Identidad:  $($identity.Arn)"
Write-Host "Region:     $Region"
Write-Host ""
Write-Host "Se crearan o verificaran estos streams:"
foreach ($stream in $streams) {
    Write-Host "  $($stream.Name): $($stream.ShardCount) shards, $($stream.RetentionHours) horas de retencion"
}
Write-Host "  Alarma: $OrdersStreamName-iterator-age"
Write-Host "Los streams existentes no se escalan ni se cambia su retencion."
Write-Host "Las tablas de leases KCL se crean cuando arrancan los consumidores."
if (-not [string]::IsNullOrWhiteSpace($CloudFormationStackName)) {
    Write-Host "Stack CloudFormation dedicado: $CloudFormationStackName"
}

if (-not $WhatIfPreference) {
    $confirmation = Read-Host 'Para continuar, escribe CONFIRMAR'
    if ($confirmation -cne "CONFIRMAR") {
        throw "Confirmacion incorrecta. No se creo ningun recurso."
    }
}

if (-not [string]::IsNullOrWhiteSpace($CloudFormationStackName)) {
    $templatePath = Join-Path $PSScriptRoot "..\infra\aws-skates-kinesis.yaml"
    if (-not (Test-Path -LiteralPath $templatePath -PathType Leaf)) {
        throw "No se encontro la plantilla CloudFormation: $templatePath"
    }

    $stackJson = Invoke-AwsCli -Arguments @(
        "cloudformation", "describe-stacks", "--stack-name", $CloudFormationStackName
    ) -AllowMissing
    if ($null -ne $stackJson) {
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
            throw "El stack contiene recursos ajenos a Kinesis: $typeList. No se actualizo el stack."
        }
    }

    if ($PSCmdlet.ShouldProcess(
            "CloudFormation stack $CloudFormationStackName",
            "Desplegar streams, shards y alarma desde la plantilla Kinesis")) {
        $parameterOverrides = @(
            "OrdersStreamName=$OrdersStreamName",
            "OrdersShardCount=$OrdersShardCount",
            "OrdersRetentionHours=$OrdersRetentionHours",
            "InventoryStreamName=$InventoryStreamName",
            "InventoryShardCount=$InventoryShardCount",
            "InventoryRetentionHours=$InventoryRetentionHours",
            "DeadLetterStreamName=$DeadLetterStreamName",
            "DeadLetterShardCount=$DeadLetterShardCount",
            "DeadLetterRetentionHours=$DeadLetterRetentionHours",
            "IteratorAgeThresholdMilliseconds=$IteratorAgeThresholdMilliseconds",
            "AlarmEvaluationPeriods=$AlarmEvaluationPeriods",
            "AlarmPeriodSeconds=$AlarmPeriodSeconds"
        )
        Invoke-AwsCli -Arguments (@(
            "cloudformation", "deploy",
            "--template-file", (Resolve-Path -LiteralPath $templatePath).Path,
            "--stack-name", $CloudFormationStackName,
            "--parameter-overrides"
        ) + $parameterOverrides + @("--no-fail-on-empty-changeset")) | Out-Null

        foreach ($stream in $streams) {
            $summaryJson = Invoke-AwsCli -Arguments @(
                "kinesis", "describe-stream-summary", "--stream-name", $stream.Name
            )
            $summary = ($summaryJson | ConvertFrom-Json).StreamDescriptionSummary
            if ($summary.OpenShardCount -ne $stream.ShardCount) {
                throw "El stream $($stream.Name) existe con $($summary.OpenShardCount) shards; se solicitaron $($stream.ShardCount)."
            }
            Write-Host "Stream verificado: $($stream.Name), $($summary.OpenShardCount) shards, estado $($summary.StreamStatus)."
        }
        Write-Host "Despliegue CloudFormation completado: $CloudFormationStackName"
    }
    return
}

foreach ($stream in $streams) {
    $streamJson = Invoke-AwsCli -Arguments @(
        "kinesis", "describe-stream-summary", "--stream-name", $stream.Name
    ) -AllowMissing

    if ($null -eq $streamJson) {
        if (-not $PSCmdlet.ShouldProcess(
                "Kinesis stream $($stream.Name)",
                "Crear con $($stream.ShardCount) shards y $($stream.RetentionHours) horas de retencion")) {
            continue
        }

        Invoke-AwsCli -Arguments @(
            "kinesis", "create-stream",
            "--stream-name", $stream.Name,
            "--shard-count", "$($stream.ShardCount)"
        ) | Out-Null
        Invoke-AwsCli -Arguments @(
            "kinesis", "wait", "stream-exists", "--stream-name", $stream.Name
        ) | Out-Null

        if ($stream.RetentionHours -gt 24) {
            Invoke-AwsCli -Arguments @(
                "kinesis", "increase-stream-retention-period",
                "--stream-name", $stream.Name,
                "--retention-period-hours", "$($stream.RetentionHours)"
            ) | Out-Null
        }
    }
    else {
        Invoke-AwsCli -Arguments @(
            "kinesis", "wait", "stream-exists", "--stream-name", $stream.Name
        ) | Out-Null
    }

    $summaryJson = Invoke-AwsCli -Arguments @(
        "kinesis", "describe-stream-summary", "--stream-name", $stream.Name
    )
    $summary = ($summaryJson | ConvertFrom-Json).StreamDescriptionSummary
    if ($summary.OpenShardCount -ne $stream.ShardCount) {
        throw "El stream $($stream.Name) existe con $($summary.OpenShardCount) shards; se solicitaron $($stream.ShardCount). No se modifico."
    }
    Write-Host "Stream verificado: $($stream.Name), $($summary.OpenShardCount) shards, retencion $($summary.RetentionPeriodHours) horas, estado $($summary.StreamStatus)."
}

$alarmName = "$OrdersStreamName-iterator-age"
if ($PSCmdlet.ShouldProcess("CloudWatch alarm $alarmName", "Crear o actualizar alarma de retraso Kinesis")) {
    Invoke-AwsCli -Arguments @(
        "cloudwatch", "put-metric-alarm",
        "--alarm-name", $alarmName,
        "--alarm-description", "Antiguedad de lectura elevada en el stream de ordenes.",
        "--namespace", "AWS/Kinesis",
        "--metric-name", "GetRecords.IteratorAgeMilliseconds",
        "--dimensions", "Name=StreamName,Value=$OrdersStreamName",
        "--statistic", "Maximum",
        "--period", "$AlarmPeriodSeconds",
        "--evaluation-periods", "$AlarmEvaluationPeriods",
        "--threshold", "$IteratorAgeThresholdMilliseconds",
        "--comparison-operator", "GreaterThanThreshold",
        "--treat-missing-data", "notBreaching",
        "--no-actions-enabled"
    ) | Out-Null
}

Write-Host ""
Write-Host "Provisionamiento Kinesis completado. Verifica los recursos en la misma cuenta y region."
