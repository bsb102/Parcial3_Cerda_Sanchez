package cl.duoc.backendskatesapp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import cl.duoc.backendskatesapp.config.KinesisProperties;

@SpringBootApplication
@EnableConfigurationProperties(KinesisProperties.class)
public class BackendSkatesAppApplication {

	public static void main(String[] args) {
		SpringApplication.run(BackendSkatesAppApplication.class, args);
	}

}
