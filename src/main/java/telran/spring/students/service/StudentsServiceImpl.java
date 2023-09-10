package telran.spring.students.service;

import java.lang.reflect.Array;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import telran.spring.exceptions.NotFoundException;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.aggregation.*;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.bson.Document;
import org.springframework.data.mongodb.core.aggregation.*;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;
import org.springframework.data.mongodb.core.aggregation.GroupOperation;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.aggregation.SortOperation;
import org.springframework.data.mongodb.core.aggregation.UnwindOperation;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import telran.spring.students.repo.StudentRepository;
import telran.spring.students.docs.StudentDoc;
import telran.spring.students.dto.IdName;
import telran.spring.students.dto.IdNameMarks;
import telran.spring.students.dto.Mark;
import telran.spring.students.dto.MarksBucket;
import telran.spring.students.dto.Student;
import telran.spring.students.dto.SubjectMark;


@RequiredArgsConstructor
@Slf4j
@Service
public class StudentsServiceImpl implements StudentsService {
	private static final Integer GOOD_MARK_THRESH0LD = 80;
	 private static final String AVG_SCORE_FIELD = "avgScore";
	private static final String SUM_SCORE_FIELD = "SumScore";
	final MongoTemplate mongoTemplate;
	final StudentRepository studentRepo;
	@Value("${app.students.mark.good:80}")
	int goodMark;
	@Override
	@Transactional(readOnly = false)
	public Student addStudent(Student student) {
		if (studentRepo.existsById(student.id())) {
			throw new IllegalStateException(String.format("student with id %d already exists", student.id()));
		}
		StudentDoc studentDoc = StudentDoc.of(student);
		Student studentRes = studentRepo.save(studentDoc).build();
		log.trace("student {} has been added", studentRes);
		return studentRes;
	}

	@Override
	@Transactional(readOnly = false)
	public void addMark(long studentId, Mark mark) {
		StudentDoc studentDoc = studentRepo.findById(studentId).orElseThrow(() ->
		new NotFoundException(String.format("student with id %d doesn't exist", studentId)));
		List<Mark>marks = studentDoc.getMarks();
		marks.add(mark);
		studentRepo.save(studentDoc);

	}

	@Override
	public List<Mark> getMarksStudentSubject(long studentId, String subject) {
		List<Mark> res = Collections.emptyList();
		SubjectMark allMarks = studentRepo.findByIdAndMarksSubjectEquals(studentId, subject);
		if(allMarks != null) {
			res = allMarks.getMarks().stream().filter(m -> m.subject().equals(subject)).toList();
		}
		return res;
		
	}

	@Override
	public List<Mark> getMarksStudentDates(long studentId, LocalDate date1, LocalDate date2) {
		List<Mark> res = Collections.emptyList();
		SubjectMark allMarks = studentRepo.findByIdAndMarksDateBetween(studentId, date1, date2);
		if(allMarks != null) {
			res = allMarks.getMarks().stream().filter(m -> {
				LocalDate date = m.date();
				return date.compareTo(date1) >= 0 && date.compareTo(date2) <= 0;
			}).toList();
		}
		return res;
	}

	@Override
	public List<Student> getStudentsPhonePrefix(String phonePrefix) {
		
		return studentRepo.findStudentsPhonePrefix(phonePrefix).stream().map(StudentDoc::build).toList();
		
	}

	@Override
	public List<IdName> getStudentsAllScoresGreater(int score) {
		
		return studentRepo.findStudentsAllMarksGreater(score);
	}

	@Override
	public List<Long> removeStudentsWithFewMarks(int nMarks) {
		List<StudentDoc> studentsRemoved = studentRepo.removeStudentsFewMarks(nMarks);
		return studentsRemoved.stream().map(StudentDoc::getId).toList();
	}

	@Override
	public List<IdName> getStudentsScoresSubjectGreater(int score, String subject) {
		return studentRepo.findStudentsMarksSubjectGreater(score, subject);
	}

	@Override
	public List<Long> removeStudentsNoLowMarks(int score) {
	List<StudentDoc> studentsRemoved = studentRepo.removeStudentsNoLowMarks(score);
	return studentsRemoved.stream().map(StudentDoc::getId).toList();
	}
	@Override
	public double getStudentsAvgScore() {
		UnwindOperation unwindOperation = unwind("marks");
		GroupOperation groupOperation = group().avg("marks.score").as("avgScore");
		Aggregation pipeLine = newAggregation(List.of(unwindOperation, groupOperation));
		var aggregationResult = mongoTemplate.aggregate(pipeLine, StudentDoc.class,Document.class);
		double res = aggregationResult.getUniqueMappedResult().getDouble("avgScore");
		return res;
	}
	
	@Override
	public List<IdName> getGoodStudents() {
		log.debug("good mark threshold is {} ", goodMark);
		return getStudentsAvgMarkGreater(goodMark);
	}
	@Override
	  public List<IdName> getStudentsAvgMarkGreater(int score) {
	    UnwindOperation unwindOperation = unwind("marks");
	    GroupOperation groupOperation = group("id", "name").avg("marks.score").as(AVG_SCORE_FIELD);
	    MatchOperation matchOperation = match(Criteria.where(AVG_SCORE_FIELD).gt(score));
	    SortOperation sortOperation = sort(Direction.DESC, AVG_SCORE_FIELD);
	    ProjectionOperation projectionOperation = project().andExclude(AVG_SCORE_FIELD);
	    Aggregation pipeLine = newAggregation(List.of(unwindOperation, groupOperation, matchOperation, sortOperation, projectionOperation));
	    var aggregationResult = mongoTemplate.aggregate(pipeLine, StudentDoc.class, Document.class);
	    List<Document> resultDocument = aggregationResult.getMappedResults();    
	    return resultDocument.stream().map(this::toIdName).toList();
	  
	}
	IdName toIdName(Document document) {
		return new IdName() {
			Document idDocument = document.get("_id", Document.class);
			
			@Override
			public String getName() {
				return idDocument.getString("name");
			}
			@Override
			public long getId() {
				
				return idDocument.getLong("id");
			}
			};
	}

	@Override
	public List<IdNameMarks> findStudents(String jsonQuery) {
		BasicQuery query = new BasicQuery(jsonQuery);
		List<StudentDoc> students = mongoTemplate.find(query, StudentDoc.class);
		
		return students.stream().map(this::toIdNameMarks).toList();
	}
	IdNameMarks toIdNameMarks(StudentDoc studentDoc) {
		return new IdNameMarks() {

			@Override
			public long getId() {
				
				return studentDoc.getId();
			}

			@Override
			public String getName() {
				
				return studentDoc.getName();
			}

			@Override
			public List<Mark> getMarks() {
				
				return studentDoc.getMarks();
			}
			
		  };
		}

	@Override
	public List<IdNameMarks> getBestStudents(int nStudents) {
		 UnwindOperation unwindOperation = unwind("marks");
		    GroupOperation groupOperation = group("id", "name").sum("marks.score").as(SUM_SCORE_FIELD);
		     SortOperation sortOperation = sort(Direction.DESC, SUM_SCORE_FIELD);
		     LimitOperation limitOperation = limit(nStudents);
		    ProjectionOperation projectionOperation = project().andExclude(SUM_SCORE_FIELD);
		    Aggregation pipeLine = newAggregation(List.of(unwindOperation, groupOperation, sortOperation, limitOperation, projectionOperation));
		    var aggregationResult = mongoTemplate.aggregate(pipeLine, StudentDoc.class, Document.class);
		    List<Document> resultDocument = aggregationResult.getMappedResults();    
		    List<IdName>idNameList = resultDocument.stream().map(this::toIdName).toList();
		    List<StudentDoc> students = findStudentsByIdName(idNameList);
			return students.stream().map(this::toIdNameMarks).toList();
	}

	private List<StudentDoc> findStudentsByIdName(List<IdName> idNameList) {
		String[]idArray = new String[idNameList.size()];
		for (int i = 0; i < idNameList.size(); i++) {
			idArray[i] = String.format("{id:%d}",idNameList.get(i).getId());
		}
		 String jsonQuery = String.format("{ $or: [%s] }",String.join(",", idArray));
		BasicQuery query = new BasicQuery(jsonQuery);
		List<StudentDoc> studentsList = mongoTemplate.find(query, StudentDoc.class);
		return sortStudentList(studentsList, idNameList);
	}

	private List<StudentDoc> sortStudentList(List<StudentDoc> studentsList, List<IdName> idNameList) {
		List<StudentDoc> sortedList = new ArrayList<StudentDoc>();
		idNameList.stream().forEach(e -> sortedList.add(studentsList.stream().filter(el -> el.getId() == e.getId()).toList().get(0)));
		return sortedList;
	}

	@Override
	public List<IdNameMarks> getworstStudents(int nStudents) {
		UnwindOperation unwindOperation = unwind("marks",true);
	    GroupOperation groupOperation = group("id", "name").sum("marks.score").as(SUM_SCORE_FIELD);
	     SortOperation sortOperation = sort(Direction.ASC, SUM_SCORE_FIELD);
	     LimitOperation limitOperation = limit(nStudents);
	    ProjectionOperation projectionOperation = project().andExclude(SUM_SCORE_FIELD);
	    Aggregation pipeLine = newAggregation(List.of(unwindOperation, groupOperation, sortOperation, limitOperation, projectionOperation));
	    var aggregationResult = mongoTemplate.aggregate(pipeLine, StudentDoc.class, Document.class);
	    List<Document> resultDocument = aggregationResult.getMappedResults();    
	    List<IdName>idNameList = resultDocument.stream().map(this::toIdName).toList();
	    List<StudentDoc> students = findStudentsByIdName(idNameList);
		return students.stream().map(this::toIdNameMarks).toList();
	}

	@Override
	public List<IdNameMarks> getBestStudentsSubject(int nStudents, String subject) {
		UnwindOperation unwindOperation = unwind("marks");
		MatchOperation matchOperation = match(Criteria.where("marks.subject").is(subject));
	    GroupOperation groupOperation = group("id", "name").sum("marks.score").as(SUM_SCORE_FIELD);
	     SortOperation sortOperation = sort(Direction.DESC, SUM_SCORE_FIELD);
	     LimitOperation limitOperation = limit(nStudents);
	    ProjectionOperation projectionOperation = project().andExclude(SUM_SCORE_FIELD);
	    Aggregation pipeLine = newAggregation(List.of(unwindOperation,matchOperation, groupOperation, sortOperation, limitOperation, projectionOperation));
	    var aggregationResult = mongoTemplate.aggregate(pipeLine, StudentDoc.class, Document.class);
	    List<Document> resultDocument = aggregationResult.getMappedResults();    
	    List<IdName>idNameList = resultDocument.stream().map(this::toIdName).toList();
	    List<StudentDoc> students = findStudentsByIdName(idNameList);
		return students.stream().map(this::toIdNameMarks).toList();
	}

	@Override
	public List<MarksBucket> scoresDistribution(int nBuckets) {
		UnwindOperation unwindOperation = unwind("marks");
		BucketAutoOperation bucketOperation = bucketAuto("marks.score", nBuckets);
		Aggregation pipeLine = newAggregation(List.of(unwindOperation, bucketOperation));
		var aggregationResult = mongoTemplate.aggregate(pipeLine, StudentDoc.class, Document.class);
		List<Document> resultDocument = aggregationResult.getMappedResults();
		log.debug(resultDocument.get(0).toJson());
		return resultDocument.stream().map(this::toMarksBucket).toList();
	}
	MarksBucket toMarksBucket(Document document) {
		Document rangeDocument = document.get("_id", Document.class);
		int min = rangeDocument.getInteger("min");
		int max = rangeDocument.getInteger("max");
		int count = document.getInteger("count");
		return new MarksBucket(min, max, count); 
	}
}
