package telran.spring.students.service;

import java.time.LocalDate;
import java.util.List;

import telran.spring.students.docs.StudentDoc;
import telran.spring.students.dto.*;

public interface StudentsService {
	Student addStudent(Student student);
	void addMark(long studentId, Mark mark);
	List<Mark> getMarksStudentSubject(long studentId, String subject);
	List<Mark> getMarksStudentDates(long studentId, LocalDate date1, LocalDate date2);
	List<StudentDoc> getStudentsPhonePrefix(String phonePrefix);
	List<IdName> getStudentsAllScoresGreater(int score);
	List<Long> removeStudentsWithFewMarks(int nMarks);
	List<IdName> getStudentsScoresSubjectGreater(int score, String subject);
	List<Long> removeStudentsNoLowMarks(int score);
	double getStudentsAvgScore();
	List<IdName> getGoodStudents();
	List<IdName> getStudentsAvgMarkGreater(int sccore);
	
	
}