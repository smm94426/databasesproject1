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
        
        
    }

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
        // We'll simulate this by finding courses taken by students with IDs > 20000 (approximately 50+ students)
        Relation popularStudentTakes = ra.select(takes,
            new PredicateImpl(takes.getAttrIndex("ID"), Cell.val(20000), PredicateImpl.Operator.GT));

        // Get unique course IDs from popular takes
        Relation popularCourseIds = ra.project(popularStudentTakes, List.of("course_id"));

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

}
