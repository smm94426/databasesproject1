package uga.csx370.mydbimpl;

import java.util.ArrayList;
import java.util.List;

import uga.csx370.mydb.*;

public class Driver {

    public static void main(String[] args) {
        RA ra = new RAImpl();

        //Load tables
        Relation student = new RelationBuilder()
                .attributeNames(List.of("ID","name","dept_name","tot_cred"))
                .attributeTypes(List.of(Type.STRING, Type.STRING, Type.STRING, Type.INTEGER))
                .build();
        student.loadData("C:/Users/seanf/mysql_exports/student_export.csv");

        Relation advisor = new RelationBuilder()
                .attributeNames(List.of("s_ID","i_ID"))
                .attributeTypes(List.of(Type.STRING, Type.STRING))
                .build();
        advisor.loadData("C:/Users/seanf/mysql_exports/advisor_export.csv");

        Relation teaches = new RelationBuilder()
                .attributeNames(List.of("ID","course_id","sec_id","semester","year"))
                .attributeTypes(List.of(Type.STRING, Type.STRING, Type.STRING, Type.STRING, Type.INTEGER))
                .build();
        teaches.loadData("C:/Users/seanf/mysql_exports/teaches_export.csv");

        Relation takes = new RelationBuilder()
                .attributeNames(List.of("ID","course_id","sec_id","semester","year","grade"))
                .attributeTypes(List.of(Type.STRING, Type.STRING, Type.STRING, Type.STRING, Type.INTEGER, Type.STRING))
                .build();
        takes.loadData("C:/Users/seanf/mysql_exports/takes_export.csv");

        // Rename for natural join
        Relation studentS = new RelationBuilder()
                .attributeNames(List.of("s_ID","name","dept_name","tot_cred"))
                .attributeTypes(student.getTypes())
                .build();
        for (int r = 0; r < student.getSize(); r++) {
            studentS.insert(new ArrayList<>(student.getRow(r)));
        }

        Relation teachesI = new RelationBuilder()
                .attributeNames(List.of("i_ID","course_id","sec_id","semester","year"))
                .attributeTypes(teaches.getTypes())
                .build();
        for (int r = 0; r < teaches.getSize(); r++) {
            teachesI.insert(new ArrayList<>(teaches.getRow(r)));
        }

        Relation takesS = new RelationBuilder()
                .attributeNames(List.of("s_ID","course_id","sec_id","semester","year","grade"))
                .attributeTypes(takes.getTypes())
                .build();
        for (int r = 0; r < takes.getSize(); r++) {
            takesS.insert(new ArrayList<>(takes.getRow(r)));
        }

        //Do natural joins
        Relation sJoinA = ra.join(studentS, advisor);
        Relation sJoinATeach = ra.join(sJoinA, teachesI);
        Relation fullJoin = ra.join(sJoinATeach, takesS);

        //Project only ID and Name
        Relation result = ra.project(fullJoin, List.of("s_ID", "name"));

        // Print 50 rows
        System.out.println("Retrieve the names and IDs of students took a course taught by their own advisor.");
        int maxRows = Math.min(50, result.getSize());
        for (int r = 0; r < maxRows; r++) {
            System.out.println(result.getRow(r));
        }
    }
}
