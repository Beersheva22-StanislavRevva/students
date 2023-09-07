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
	List<IdName> getSudentsAllScoresGreater(int score);
	List<Long> removeStudentsWithFewMarks(int nMarks);
	double getStudentsAvgScore();
	List<IdName> getGoodStudents(); //students having avg scores greater than the avg score for all students
	
	
}