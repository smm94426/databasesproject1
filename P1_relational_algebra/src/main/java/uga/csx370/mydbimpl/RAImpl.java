package uga.csx370.mydbimpl;

import java.util.List;

import uga.csx370.mydb.Predicate;
import uga.csx370.mydb.RA;
import uga.csx370.mydb.Relation;

public class RAImpl implements RA {

    @Override
    public Relation select(Relation rel, Predicate p) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'select'");
    }

    @Override
    public Relation project(Relation rel, List<String> attrs) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'project'");
    }

    @Override
    public Relation union(Relation rel1, Relation rel2) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'union'");
    }

    @Override
    public Relation intersect(Relation rel1, Relation rel2) {
        // A and B = A - (A - B)
        return diff(rel1, diff(rel1, rel2));
    }

    @Override
    public Relation diff(Relation rel1, Relation rel2) {
        //make new relation with same schema as rel1
        Relation retrel = new RelationBuilder()
            .attributeNames(rel1.getAttrs())
            .attributeTypes(rel1.getTypes())
            .build();

        //for row in rel1, check if exists in rel2
        for (int i = 0; i < rel1.getSize(); i++) {
            int found = 0;
            for (int j = 0; j < rel2.getSize(); j++) {
                if ((rel1.getRow(i)).equals(rel2.getRow(j))) {
                    found = 1;
                    break;
                }
            }
            //if not in rel2 add to retrel
            if (found == 0) {
                if (retrel.getSize() == 0) { //if retrel empty, add
                    retrel.insert(rel1.getRow(i));
                } else { //else check 4 dupes
                    int match = 0;
                    for (int k = 0; k < retrel.getSize(); k++) {
                        if ((rel1.getRow(i)).equals(retrel.getRow(k))) {
                            match = 1;
                            break;
                        }
                    }
                    //if not dupe, add to retrel
                    if (match == 0) {
                        retrel.insert(rel1.getRow(i));
                    }
                }
            }
        }
        return retrel;
    }

    @Override
    public Relation rename(Relation rel, List<String> origAttr, List<String> renamedAttr) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'rename'");
    }

    @Override
    public Relation cartesianProduct(Relation rel1, Relation rel2) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'cartesianProduct'");
    }

    @Override
    public Relation join(Relation rel1, Relation rel2) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'join'");
    }

    @Override
    public Relation join(Relation rel1, Relation rel2, Predicate p) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'join'");
    }

}
