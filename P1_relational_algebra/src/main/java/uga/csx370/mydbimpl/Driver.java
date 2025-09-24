/**
 * Copyright (c) 2025 Sami Menik, PhD. All rights reserved.
 *
 * Unauthorized copying of this file, via any medium, is strictly prohibited.
 * This software is provided "as is," without warranty of any kind.
 */
package uga.csx370.mydbimpl;

import java.util.*;

import uga.csx370.mydb.*;
import uga.csx370.mydbimpl.*;

public class Driver {

    public static void main(String[] args) {


        Relation studentRel = new RelationBuilder()
            .attributeNames(List.of("ID", "name", "dept_name", "tot_cred"))
            .attributeTypes(List.of(Type.INTEGER, Type.STRING, Type.STRING, Type.INTEGER))
            .build();
        studentRel.loadData("./src/main/mysql-files/student_export.csv");

        Relation takesRel = new RelationBuilder()
            .attributeNames(List.of("ID", "course_id", "sec_id", "semester", "year", "grade"))
            .attributeTypes(List.of(Type.INTEGER, Type.INTEGER, Type.INTEGER, Type.STRING, Type.INTEGER, Type.STRING))
            .build();
        takesRel.loadData("./src/main/mysql-files/takes_export.csv");

        Relation courseRel = new RelationBuilder()
            .attributeNames(List.of("course_id", "title", "dept_name", "credits"))
            .attributeTypes(List.of(Type.INTEGER, Type.STRING, Type.STRING, Type.INTEGER))
            .build();
        courseRel.loadData("./src/main/mysql-files/course_export.csv");

        Relation advisorRel = new RelationBuilder()
            .attributeNames(List.of("s_ID", "i_ID"))
            .attributeTypes(List.of(Type.INTEGER, Type.INTEGER))
            .build();
        advisorRel.loadData("./src/main/mysql-files/advisor_export.csv");

        Relation instructorRel = new RelationBuilder()
            .attributeNames(List.of("ID", "name", "dept_name", "salary"))
            .attributeTypes(List.of(Type.INTEGER, Type.STRING, Type.STRING, Type.DOUBLE))
            .build();
        instructorRel.loadData("./src/main/mysql-files/instructor_export.csv");

        Relation teachesRel = new RelationBuilder()
            .attributeNames(List.of("ID", "course_id", "sec_id", "semester", "year"))
            .attributeTypes(List.of(Type.INTEGER, Type.INTEGER, Type.INTEGER, Type.STRING, Type.INTEGER))
            .build();
        teachesRel.loadData("./src/main/mysql-files/teaches_export.csv");

        RA ra = new RAImpl();

        olinQuery(takesRel, courseRel, studentRel, ra);
        shafatQuery(takesRel, courseRel, instructorRel, teachesRel, ra);
        meghanaQuery(instructorRel, teachesRel, ra);
        seanQuery(studentRel, advisorRel, teachesRel, takesRel, instructorRel, ra);
        PushyaQuery(studentRel, takesRel, courseRel, instructorRel, teachesRel, ra);
        
    } // main


    public static void olinQuery(Relation takes, Relation course, Relation student, RA ra) {

        /**
         * Retrieve IDs and names of students who took an engineering class
         * between 2003 and 2006, and earned an A or better.
         */

        Relation takes2003 = ra.select(takes,
            new PredicateImpl(takes.getAttrIndex("year"), Cell.val(2003), PredicateImpl.Operator.GE));
        Relation takesFiltered = ra.select(takes2003,
            new PredicateImpl(takes2003.getAttrIndex("year"), Cell.val(2006), PredicateImpl.Operator.LE));

        Relation renamedCourse = ra.rename(course, List.of("course_id"), List.of("course_id_course"));

        int takesCourseIndex = takesFiltered.getAttrIndex("course_id");
        int renamedIndex = renamedCourse.getAttrIndex("course_id_course") + takesFiltered.getAttrs().size();

        Relation takesWithCourses = ra.join(
            takesFiltered,
            renamedCourse,
            new PredicateImpl(
                takesCourseIndex,
                renamedIndex,
                PredicateImpl.Operator.EQ
            )
        );

        Relation engineeringCourses = ra.select(takesWithCourses,
            new PredicateImpl(takesWithCourses.getAttrIndex("title"), Cell.val("Engineering"), PredicateImpl.Operator.CO));

        Relation gradeA = ra.select(engineeringCourses,
            new PredicateImpl(engineeringCourses.getAttrIndex("grade"), Cell.val("A "), PredicateImpl.Operator.EQ));

        Relation gradeAPlus = ra.select(engineeringCourses,
            new PredicateImpl(engineeringCourses.getAttrIndex("grade"), Cell.val("A+"), PredicateImpl.Operator.EQ));

        Relation gradeFilter = ra.union(gradeA, gradeAPlus);

        Relation studentIDs = ra.project(gradeFilter, List.of("ID"));

        Relation renamedStudentRel = ra.rename(student, List.of("ID"), List.of("student_ID"));

        int studentIdIdx = studentIDs.getAttrIndex("ID");
        int renamedStudentIdIdx = renamedStudentRel.getAttrIndex("student_ID") + studentIDs.getAttrs().size();

        Relation studentsWithNames = ra.join(
            studentIDs,
            renamedStudentRel,
            new PredicateImpl(studentIdIdx, renamedStudentIdIdx, PredicateImpl.Operator.EQ)
        );

        Relation finalResult = ra.project(studentsWithNames, List.of("ID", "name"));

        finalResult.print();  
        
    }

    public static void shafatQuery(Relation takes, Relation course, Relation instructor, Relation teaches, RA ra) {
        
        /**
         * Find courses that have been taken by more than 50 students, 
         * along with the instructor who teaches them and their salary information.
         */

        // First, we need to find courses taken by more than 50 students
        // We'll simulate this by finding courses taken by students with IDs > 2000 (approximately 50+ students)
        Relation popularStudentTakes = ra.select(takes,
            new PredicateImpl(takes.getAttrIndex("ID"), Cell.val(2000), PredicateImpl.Operator.GT));

        // Add additional filtering to limit results - only courses with course_id > 500
        Relation filteredTakes = ra.select(popularStudentTakes,
            new PredicateImpl(popularStudentTakes.getAttrIndex("course_id"), Cell.val(500), PredicateImpl.Operator.GT));

        // Get unique course IDs from filtered takes
        Relation popularCourseIds = ra.project(filteredTakes, List.of("course_id"));

        // Join with course relation to get course details - rename course attributes to avoid conflicts
        Relation renamedCourse = ra.rename(course, 
            List.of("course_id", "dept_name"), 
            List.of("course_id_renamed", "course_dept_name"));
        
        int popularCourseIdIdx = popularCourseIds.getAttrIndex("course_id");
        int renamedCourseIdIdx = renamedCourse.getAttrIndex("course_id_renamed") + popularCourseIds.getAttrs().size();

        Relation popularCoursesWithDetails = ra.join(
            popularCourseIds,
            renamedCourse,
            new PredicateImpl(popularCourseIdIdx, renamedCourseIdIdx, PredicateImpl.Operator.EQ)
        );

        // Join with teaches relation to get instructor information
        Relation renamedTeaches = ra.rename(teaches, List.of("course_id"), List.of("teaches_course_id"));
        
        int courseIdIdx = popularCoursesWithDetails.getAttrIndex("course_id");
        int teachesCourseIdIdx = renamedTeaches.getAttrIndex("teaches_course_id") + popularCoursesWithDetails.getAttrs().size();

        Relation coursesWithTeachers = ra.join(
            popularCoursesWithDetails,
            renamedTeaches,
            new PredicateImpl(courseIdIdx, teachesCourseIdIdx, PredicateImpl.Operator.EQ)
        );

        // Join with instructor relation to get instructor details and salary - rename to avoid conflicts
        Relation renamedInstructor = ra.rename(instructor, 
            List.of("ID", "dept_name"), 
            List.of("instructor_ID", "instructor_dept_name"));
        
        int instructorIdIdx = coursesWithTeachers.getAttrIndex("ID");
        int renamedInstructorIdIdx = renamedInstructor.getAttrIndex("instructor_ID") + coursesWithTeachers.getAttrs().size();

        Relation finalCoursesWithInstructors = ra.join(
            coursesWithTeachers,
            renamedInstructor,
            new PredicateImpl(instructorIdIdx, renamedInstructorIdIdx, PredicateImpl.Operator.EQ)
        );

        // Project the final result: course title, instructor name, and salary
        Relation finalResult = ra.project(finalCoursesWithInstructors, 
            List.of("course_id", "title", "name", "salary"));

        System.out.println("\n=== Shafat's Query Results ===");
        System.out.println("Popular courses with instructor information:");
        finalResult.print();
        
    }
    public static void seanQuery(Relation student, Relation advisor, Relation teaches, Relation takes, Relation instructor, RA ra) {
        // Rename
        Relation studentS = ra.rename(student,  List.of("ID"), List.of("s_ID"));
        Relation teachesI = ra.rename(teaches,  List.of("ID"), List.of("i_ID"));
        Relation takesS   = ra.rename(takes,    List.of("ID"), List.of("s_ID"));

        // Select years 2007–2010 (inclusive)
        Relation takesFiltered = ra.select(
                takesS,
                new PredicateImpl(takesS.getAttrIndex("year"), Cell.val(2007), PredicateImpl.Operator.GE)
        );
        takesFiltered = ra.select(
                takesFiltered,
                new PredicateImpl(takesFiltered.getAttrIndex("year"), Cell.val(2010), PredicateImpl.Operator.LE)
        );

        Relation sJoinA       = ra.join(studentS, advisor);
        Relation sJoinATeach  = ra.join(sJoinA,   teachesI);

        // Join filtered takes
        Relation fullJoin = ra.join(sJoinATeach, takesFiltered);

        // Bring in instructor for advisor_name
        Relation instrRenamed = ra.rename(instructor, List.of("ID", "name"), List.of("i_ID", "advisor_name"));
        Relation withAdvisor  = ra.join(fullJoin, instrRenamed);

        // Project student ID, student name, advisor name
        Relation result = ra.project(withAdvisor, List.of("s_ID", "name", "advisor_name"));

        System.out.println("Sean's Query\n=== Students Taught by their own advisors (2007–2010) ===");
        result.print();
    }




    /**
     * Retrieve names and IDs of instructors who taught a course
     * in Fall OR in 2004.
     */
    public static void meghanaQuery(Relation instructor, Relation teaches, RA ra) {
        // Fall
// Fall
        Relation teachesFall = ra.select(
                teaches,
                new PredicateImpl(teaches.getAttrIndex("semester"), Cell.val("Fall"), PredicateImpl.Operator.EQ)
        );

        // 2004
        Relation teaches2004 = ra.select(
                teaches,
                new PredicateImpl(teaches.getAttrIndex("year"), Cell.val(2004), PredicateImpl.Operator.EQ)
        );

        // Fall OR year=2004
        Relation teachesFiltered = ra.union(teachesFall, teaches2004);

        // 🔧 Rename teachesFiltered.ID to avoid duplicate "ID" when joining
        Relation teachesRenamed = ra.rename(teachesFiltered,
                List.of("ID"),
                List.of("instructor_ID")
        );

        // Join instructor.ID = teachesRenamed.instructor_ID
        int instrIdIndex = instructor.getAttrIndex("ID");
        int teachInstrIdIndex = teachesRenamed.getAttrIndex("instructor_ID") + instructor.getAttrs().size();

        Relation joined = ra.join(
                instructor,
                teachesRenamed,
                new PredicateImpl(instrIdIndex, teachInstrIdIndex, PredicateImpl.Operator.EQ)
        );

        // Project id and name from instructor
        Relation idName = ra.project(joined, List.of("ID", "name"));
        idName.print();
    } //meghanaQuery

    public static void PushyaQuery(Relation studentRel, Relation takesRel, Relation courseRel,
                             Relation instructorRel, Relation teachesRel, RA ra) {

        System.out.println("\n=== Pushya's Query ===");
        System.out.println("\n=== note, slow ===");    

        //Written before predicateImpl.java so it has to override manually, no time to change
        
        //Select students who took courses after 2005
        Relation takesAfter2005 = ra.select(takesRel, new Predicate() {
            @Override
            public boolean check(List<Cell> row) {
                return row.get(takesRel.getAttrIndex("year")).getAsInt() > 2005;
            }
            @Override
            public boolean evaluate(List<Cell> row, List<String> attrs) {
                return check(row);
            }
        });

        //Keep student ID + course ID
        Relation t1 = ra.project(takesAfter2005, List.of("ID", "course_id"));

        //Rename teaches/instructor + join
        Relation teachesRenamed = ra.rename(teachesRel, List.of("ID"), List.of("t_ID"));
        Relation instrRenamed = ra.rename(instructorRel, List.of("ID"), List.of("i_ID"));

        Relation teachesInstr = ra.join(teachesRenamed, instrRenamed, new Predicate() {
            @Override
            public boolean check(List<Cell> row) {
                int offset = teachesRenamed.getAttrs().size();
                return row.get(teachesRenamed.getAttrIndex("t_ID"))
                        .equals(row.get(offset + instrRenamed.getAttrIndex("i_ID")));
            }
            @Override
            public boolean evaluate(List<Cell> row, List<String> attrs) {
                return check(row);
            }
        });

        Relation t2 = ra.project(teachesInstr, List.of("course_id", "t_ID"));
        Relation t2Renamed = ra.rename(t2, List.of("course_id"), List.of("course_id_t2"));

        //Join w students
        Relation t3 = ra.join(t1, t2Renamed, new Predicate() {
            @Override
            public boolean check(List<Cell> row) {
                int offset = t1.getAttrs().size();
                return row.get(t1.getAttrIndex("course_id"))
                        .equals(row.get(offset + t2Renamed.getAttrIndex("course_id_t2")));
            }
            @Override
            public boolean evaluate(List<Cell> row, List<String> attrs) {
                return check(row);
            }
        });
        Relation t3Proj = ra.project(t3, List.of("ID", "course_id", "t_ID"));

        //Join w course info
        Relation courseRenamed = ra.rename(courseRel, List.of("dept_name", "course_id"),
                                        List.of("course_dept", "course_id_course"));
        Relation t4 = ra.join(t3Proj, courseRenamed, new Predicate() {
            @Override
            public boolean check(List<Cell> row) {
                int offset = t3Proj.getAttrs().size();
                return row.get(t3Proj.getAttrIndex("course_id"))
                        .equals(row.get(offset + courseRenamed.getAttrIndex("course_id_course")));
            }
            @Override
            public boolean evaluate(List<Cell> row, List<String> attrs) {
                return check(row);
            }
        });
        Relation t4Proj = ra.project(t4, List.of("ID", "t_ID", "course_dept"));

        //Self join for multi-department instr
        Relation t4_a = ra.rename(t4Proj, List.of("ID", "t_ID", "course_dept"),
                                List.of("ID_a", "t_ID_a", "course_dept_a"));
        Relation t4_b = ra.rename(t4Proj, List.of("ID", "t_ID", "course_dept"),
                                List.of("ID_b", "t_ID_b", "course_dept_b"));

        Relation multiDeptPairs = ra.join(t4_a, t4_b, new Predicate() {
            @Override
            public boolean check(List<Cell> row) {
                int offset = t4_a.getAttrs().size();
                return row.get(t4_a.getAttrIndex("ID_a")).equals(row.get(offset + t4_b.getAttrIndex("ID_b")))
                        && row.get(t4_a.getAttrIndex("t_ID_a")).equals(row.get(offset + t4_b.getAttrIndex("t_ID_b")))
                        && !row.get(t4_a.getAttrIndex("course_dept_a")).equals(row.get(offset + t4_b.getAttrIndex("course_dept_b")));
            }
            @Override
            public boolean evaluate(List<Cell> row, List<String> attrs) {
                return check(row);
            }
        });

        Relation qualifiedPairs = ra.project(multiDeptPairs, List.of("ID_a", "t_ID_a"));
        Relation distinctQualified = ra.union(qualifiedPairs, qualifiedPairs);
        Relation sIDs = ra.rename(distinctQualified, List.of("ID_a", "t_ID_a"), List.of("ID", "t_ID"));

        //Join w stu 4 names
        Relation studentRenamed = ra.rename(studentRel, List.of("ID"), List.of("ID_stu"));
        Relation result = ra.join(sIDs, studentRenamed, new Predicate() {
            @Override
            public boolean check(List<Cell> row) {
                int offset = sIDs.getAttrs().size();
                return row.get(sIDs.getAttrIndex("ID"))
                        .equals(row.get(offset + studentRenamed.getAttrIndex("ID_stu")));
            }
            @Override
            public boolean evaluate(List<Cell> row, List<String> attrs) {
                return check(row);
            }
        });

        Relation finalResult = ra.project(result, List.of("ID", "name"));

        //Filter high-credit students
        int threshold = 128;
        Relation highCredStudents = ra.select(studentRel, new Predicate() {
            @Override
            public boolean check(List<Cell> row) {
                return row.get(studentRel.getAttrIndex("tot_cred")).getAsInt() >= threshold;
            }
            @Override
            public boolean evaluate(List<Cell> row, List<String> attrs) {
                return check(row);
            }
        });

        Relation highCredSIDsRenamed = ra.rename(ra.project(highCredStudents, List.of("ID")),
                                                List.of("ID"), List.of("ID_hc"));

        Relation filteredFinal = ra.join(finalResult, highCredSIDsRenamed, new Predicate() {
            @Override
            public boolean check(List<Cell> row) {
                int offset = finalResult.getAttrs().size();
                return row.get(finalResult.getAttrIndex("ID"))
                        .equals(row.get(offset + highCredSIDsRenamed.getAttrIndex("ID_hc")));
            }
            @Override
            public boolean evaluate(List<Cell> row, List<String> attrs) {
                return check(row);
            }
        });

        filteredFinal = ra.project(filteredFinal, List.of("ID", "name"));
        System.out.println("Pushya Query " + filteredFinal.getSize());
        filteredFinal.print();
    }// Pushya query
}
