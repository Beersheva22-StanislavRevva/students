package telran.spring.students;

import java.time.LocalDate;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import static org.junit.jupiter.api.Assertions.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import static telran.spring.students.TestDbCreation.*;
import telran.spring.students.docs.StudentDoc;
import telran.spring.students.dto.IdName;
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
	@Autowired
	StudentRepository studentRepo;
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
	@Test
	void studentsPhonePrefixTest() {
		List<StudentDoc> students = studentsService.getStudentsPhonePrefix("050");
		assertEquals(3, students.size());
		StudentDoc student2 = students.get(0);
		assertNull(student2.getMarks());
		assertEquals(ID2, student2.getId());
		students.forEach(s -> assertTrue(s.getPhone().startsWith("050"))); 
	}
//	@Test
//	void studentsAllMarksGreaterTest() {
//		List<IdName> students = studentsService.getStudentsAllScoresGreater(70);
//		assertEquals(2, students.size());
//		IdName studentDoc = students.get(0);
//		
//		assertEquals(ID3, studentDoc.getId());
//		assertEquals("name3", studentDoc.getName());
//		assertEquals(ID5, students.get(1).getId());
//		
//	}
	@Test
	void studentsFewMarksTest() {
		List<Long>ids = studentsService.removeStudentsWithFewMarks(2);
		assertEquals(2, ids.size());
		assertEquals(ID4, ids.get(0));
		assertEquals(ID6, ids.get(1));
		assertNull(studentRepo.findById(ID4).orElse(null));
		assertNull(studentRepo.findById(ID6).orElse(null));
	}
//	@Test
//	void StudentsScoresSubjectGreaterTest() {
//		List<IdName> students = studentsService.getStudentsScoresSubjectGreater(70, SUBJECT1);
//		assertEquals(3, students.size());
//		IdName studentDoc = students.get(0);
//		assertEquals(ID1, studentDoc.getId());
//		assertEquals("name1", studentDoc.getName());
//		assertEquals(ID3, students.get(1).getId());
//		
//	}
//	@Test
//	void StudentsNoLowMarksTest() {
//		List<Long>ids = studentsService.remove(71);
//		assertEquals(2, ids.size());
//		assertEquals(ID4, ids.get(0));
//		assertEquals(ID6, ids.get(1));
//		assertNull(studentRepo.findById(ID4).orElse(null));
//		assertNull(studentRepo.findById(ID6).orElse(null));
//	}
	@Test
	void getAvgMarkTest() {
		assertEquals(testDbCreation.getAvgMark(),studentsService.getStudentsAvgScore(),0.1);
	}
}
