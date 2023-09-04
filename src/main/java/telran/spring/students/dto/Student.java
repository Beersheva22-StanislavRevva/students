package telran.spring.students.dto;

import jakarta.validation.constraints.NotNull;

public record Student (@NotNull Long id, String name, @NotNull String phone) {

	
}
