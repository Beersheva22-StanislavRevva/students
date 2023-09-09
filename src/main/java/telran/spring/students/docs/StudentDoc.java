package telran.spring.students.docs;
import java.util.ArrayList;
import java.util.List;
import org.springframework.data.mongodb.core.mapping.Document;
import lombok.Data;
import telran.spring.students.dto.Mark;
import telran.spring.students.dto.Student;

@Document(collection = "students")
@Data
public class StudentDoc {
	
	public StudentDoc(long id, String name, String phone, List<Mark> marks) {
		super();
		this.id = id;
		this.name = name;
		this.phone = phone;
		this.marks = marks;
	}
	private final long id;
	String name;
	String phone;
	List<Mark> marks = new ArrayList<>();;
	public static StudentDoc of(Student student) {
		return new StudentDoc(student.id(), student.name(), student.phone(), null);
	}
	public Student build() {
		return new Student(getId(), name, phone);
	}
}
