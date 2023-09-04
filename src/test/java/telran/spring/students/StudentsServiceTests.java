package telran.spring.students;

import java.time.LocalDate;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import static org.junit.jupiter.api.Assertions.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import static telran.spring.students.TestDbCreation.*;
import telran.spring.students.docs.StudentDoc;
import telran.spring.students.dto.Mark;
import telran.spring.students.dto.Student;
import telran.spring.students.dto.SubjectMark;
import telran.spring.students.repo.StudentRepository;
import telran.spring.students.service.StudentsService;

@SpringBootTest

class StudentsServiceTests {
	@Autowired
	StudentsService studentsService;
	@Autowired
	TestDbCreation testDbCreation;
	@BeforeEach
	void setUp() {
		testDbCreation.createDb();
	}
	@Test
	void studentSubjectMarksTest() {
		List<Mark> marks = studentsService.getMarksStudentSubject(ID1, SUBJECT1);
		assertEquals(2,marks.size());
		assertEquals(80, marks.get(0).score());
		assertEquals(90, marks.get(1).score());
	}
	@Test
	void studentDatesMarksTest() {
		List<Mark> marks = studentsService.getMarksStudentDates(ID1, DATE2, DATE3);
		assertEquals(2, marks.size());
		assertEquals(70, marks.get(0).score());
		assertEquals(90, marks.get(1).score());
		marks = studentsService.getMarksStudentDates(ID4, DATE2, DATE3);
		assertTrue(marks.isEmpty());
	}

}
