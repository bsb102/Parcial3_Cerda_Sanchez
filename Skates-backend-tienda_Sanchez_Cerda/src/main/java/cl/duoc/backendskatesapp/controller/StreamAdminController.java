package cl.duoc.backendskatesapp.controller;

import java.util.List;
import java.util.Map;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotBlank;

import org.springframework.context.annotation.Profile;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import cl.duoc.backendskatesapp.service.StreamAdminService;

@RestController
@RequestMapping("/api/admin/streams")
@Profile({"all", "admin"})
@PreAuthorize("hasRole('ADMIN')")
public class StreamAdminController {
    private final StreamAdminService service;

    public StreamAdminController(StreamAdminService service) {
        this.service = service;
    }

    @GetMapping
    public List<StreamAdminService.StreamSummary> list() { return service.listStreams(); }

    @PostMapping
    public StreamAdminService.StreamSummary create(@Valid @RequestBody CreateStreamRequest request) {
        return service.createStream(request.name(), request.shardCount(), request.retentionHours());
    }

    @GetMapping("/{name}")
    public StreamAdminService.StreamSummary describe(@PathVariable String name) {
        return service.describeStream(name);
    }

    @DeleteMapping("/{name}")
    public void delete(@PathVariable String name) { service.deleteStream(name); }

    @PutMapping("/{name}/shards")
    public StreamAdminService.StreamSummary updateShards(
            @PathVariable String name, @Valid @RequestBody ShardCountRequest request) {
        return service.updateShardCount(name, request.shardCount());
    }

    @PutMapping("/{name}/retention")
    public StreamAdminService.StreamSummary updateRetention(
            @PathVariable String name, @Valid @RequestBody RetentionRequest request) {
        return service.updateRetention(name, request.retentionHours());
    }

    @PostMapping("/bootstrap")
    public List<StreamAdminService.StreamSummary> bootstrap() { return service.bootstrap(); }

    @GetMapping("/consumers")
    public List<StreamAdminService.ConsumerLease> consumers() { return service.consumerLeases(); }

    @GetMapping("/delay")
    public Map<String, Object> delay(@RequestParam String streamName) { return service.iteratorAge(streamName); }

    public record CreateStreamRequest(
            @NotBlank String name, @Min(1) @Max(10000) int shardCount, @Min(24) @Max(8760) int retentionHours) { }
    public record ShardCountRequest(@Min(1) @Max(10000) int shardCount) { }
    public record RetentionRequest(@Min(24) @Max(8760) int retentionHours) { }
}
