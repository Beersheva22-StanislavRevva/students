package telran.spring.students.service;

import java.time.LocalDate;
import java.util.List;
import telran.spring.students.dto.*;

public interface StudentsService {
	Student addStudent(Student student);
	void addMark(long studentId, Mark mark);
	List<Mark> getMarksStudentSubject(long studentId, String subject);
	List<Mark> getMarksStudentDates(long studentId, LocalDate date1, LocalDate date2);
}
