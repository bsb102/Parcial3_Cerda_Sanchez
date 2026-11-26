package cl.duoc.backendskatesapp.controller;

import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Profile;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import cl.duoc.backendskatesapp.messaging.MessagingDomainEvent;
import cl.duoc.backendskatesapp.model.Skate;
import cl.duoc.backendskatesapp.service.SkateService;
import cl.duoc.backendskatesapp.streaming.event.InventoryStreamEvent;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/skate")
@Profile({"all", "catalog", "admin"})
public class SkateController {

    private final SkateService skateService;
    private final ApplicationEventPublisher eventPublisher;

    public SkateController(SkateService skateService, ApplicationEventPublisher eventPublisher) {
        this.skateService = skateService;
        this.eventPublisher = eventPublisher;
    }

    @GetMapping
    public List<Skate> listar() {
        return skateService.listarTodos();
    }

    @GetMapping("/modelos")
    public List<String> listarModelos() {
        return skateService.listarModelos();
    }

    @GetMapping("/modelo/{modelo}")
    public List<Skate> listarPorModelo(@PathVariable String modelo) {
        return skateService.listarPorModelo(modelo);
    }

    @GetMapping("/{id}")
    public Skate obtener(@PathVariable Long id) {
        return skateService.buscarPorId(id);
    }

    @PostMapping
    @PreAuthorize("hasRole('ADMIN')")
    @ResponseStatus(HttpStatus.CREATED)
    public Skate crear(@Valid @RequestBody Skate nuevo) {
        Skate creado = skateService.crear(nuevo);
        publicarActualizacionInventario(creado.getId(), "CREATED");
        eventPublisher.publishEvent(InventoryStreamEvent.create(
                "admin", "skate-" + creado.getId(), creado.getId(), 0, creado.getStock(), "CREATED"));
        return creado;
    }

    @PutMapping("/{id}")
    @PreAuthorize("hasRole('ADMIN')")
    public Skate actualizar(@PathVariable Long id, @Valid @RequestBody Skate datos) {
        Skate anterior = skateService.buscarPorId(id);
        Skate actualizado = skateService.actualizar(id, datos);
        publicarActualizacionInventario(actualizado.getId(), "UPDATED");
        eventPublisher.publishEvent(InventoryStreamEvent.create(
                "admin", "skate-" + id, id, anterior.getStock(), actualizado.getStock(), "UPDATED"));
        return actualizado;
    }

    @DeleteMapping("/{id}")
    @PreAuthorize("hasRole('ADMIN')")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void eliminar(@PathVariable Long id) {
        Skate anterior = skateService.buscarPorId(id);
        skateService.eliminar(id);
        publicarActualizacionInventario(id, "DELETED");
        eventPublisher.publishEvent(InventoryStreamEvent.create(
                "admin", "skate-" + id, id, anterior.getStock(), 0, "DELETED"));
    }

    private void publicarActualizacionInventario(Long skateId, String operation) {
        eventPublisher.publishEvent(MessagingDomainEvent.create(
                "INVENTORY_UPDATED",
                Map.of("skateId", skateId, "operation", operation)));
    }
}