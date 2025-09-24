package uga.csx370.mydbimpl;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import uga.csx370.mydb.*;
import java.util.HashSet;

import uga.csx370.mydb.Predicate;
import uga.csx370.mydb.RA;
import uga.csx370.mydb.Relation;
import uga.csx370.mydb.RelationBuilder;
import uga.csx370.mydb.Type;
import uga.csx370.mydb.Cell;


public class RAImpl implements RA {

    @Override
    public Relation select(Relation rel, Predicate p) {
        if (rel == null) {
            throw new IllegalArgumentException("rel can't be null");
        } // if 
        if (p == null) {
            throw new IllegalArgumentException("predicate can't be null");
        } // if 
        
         Relation result = new RelationBuilder()
                .attributeNames(rel.getAttrs())
                .attributeTypes(rel.getTypes())
                .build();

        for (int i = 0; i < rel.getSize(); i++) {
            List<Cell> row = rel.getRow(i);
            if (p.check(row)) {
                result.insert(row);
            } // if
        } // for
        return result;
    } // select

    public Relation project(Relation rel, List<String> attrs) {
        if (rel == null) {
            throw new IllegalArgumentException("rel can't be null");
        } // if
        if (attrs == null) {
            throw new IllegalArgumentException("attrs can't be null");
        }
        // Validate and collect indices/types
        List<Integer> indices = new ArrayList<>(attrs.size());
        List<Type> types = new ArrayList<>(attrs.size());
        for (String a : attrs) {
            if (!rel.hasAttr(a)) {
                throw new IllegalArgumentException("Attribute '" + a + "' is not present in the relation.");
            }
            int idx = rel.getAttrIndex(a);
            indices.add(idx);
            types.add(rel.getTypes().get(idx));
        }

        Relation out = new RelationBuilder()
                .attributeTypes(types)
                .attributeNames(attrs)
                .build();

        Set<List<Cell>> seen = new LinkedHashSet<>();
        for (int r = 0; r < rel.getSize(); r++) {
            List<Cell> src = rel.getRow(r);
            List<Cell> proj = new ArrayList<>(indices.size());
            for (int j : indices) proj.add(src.get(j));
            if (seen.add(proj)) {
                out.insert(new ArrayList<>(proj));
            }
        }
        return out;
    }


    @Override
    public Relation union(Relation rel1, Relation rel2) {
        // Check for null relations
        if (rel1 == null || rel2 == null) {
            throw new IllegalArgumentException("Relations cannot be null");
        }
        
        // Check compatibility - same attributes and types
        if (!rel1.getAttrs().equals(rel2.getAttrs()) || !rel1.getTypes().equals(rel2.getTypes())) {
            throw new IllegalArgumentException("Relations are not compatible");
        }
        
        // Create result relation with same schema as input relations
        Relation result = new RelationBuilder()
                .attributeNames(rel1.getAttrs())
                .attributeTypes(rel1.getTypes())
                .build();
        
        // Add all rows from rel1
        for (int i = 0; i < rel1.getSize(); i++) {
            result.insert(rel1.getRow(i));
        }
        
        // Add rows from rel2 that are not already in result (avoid duplicates)
        for (int i = 0; i < rel2.getSize(); i++) {
            List<Cell> row = rel2.getRow(i);
            if (!containsRow(result, row)) {
                result.insert(row);
            }
        }
        
        return result;
    }
    
    /**
     * Helper method to check if a relation contains a specific row
     */
    private boolean containsRow(Relation rel, List<Cell> row) {
        for (int i = 0; i < rel.getSize(); i++) {
            if (rowsEqual(rel.getRow(i), row)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Helper method to check if two rows are equal
     */
    private boolean rowsEqual(List<Cell> row1, List<Cell> row2) {
        if (row1.size() != row2.size()) {
            return false;
        }
        for (int i = 0; i < row1.size(); i++) {
            if (!row1.get(i).equals(row2.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Relation intersect(Relation rel1, Relation rel2) {
        // A and B = A - (A - B)
        return diff(rel1, diff(rel1, rel2));
    }

    @Override
    public Relation diff(Relation rel1, Relation rel2) {
        if (!rel1.getAttrs().equals(rel2.getAttrs()) || !rel1.getTypes().equals(rel2.getTypes())) {
            throw new IllegalArgumentException("Relations are not compatible");
        }
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
       if (rel == null) {
            throw new IllegalArgumentException("rel can't be null");
        } // if 
        if (origAttr == null || renamedAttr == null) {
            throw new IllegalArgumentException("Attribute lists can't be null");
        } // if
        if (origAttr.size() != renamedAttr.size()) {
            throw new IllegalArgumentException("origAttr and renamedAttr must be same size");
        } // if

        List<String> newAttrNames = new ArrayList<>(rel.getAttrs());

        for (int i = 0; i < origAttr.size(); i++) {
            String from = origAttr.get(i);
            String to = renamedAttr.get(i);

            int idx = newAttrNames.indexOf(from);
            if (idx < 0) {
                throw new IllegalArgumentException("couldn't find attr to rename: " + from);
            } // if
            newAttrNames.set(idx, to);
        } // for 

        // duplicate handling 
        Set<String> uniq = new HashSet<>(newAttrNames);
        if (uniq.size() != newAttrNames.size()) {
            throw new IllegalArgumentException("duplicate found so can't rename: " + newAttrNames);
        } // if 

        Relation renamed = new RelationBuilder()
                .attributeNames(newAttrNames)
                .attributeTypes(rel.getTypes())
                .build();

        for (int i = 0; i < rel.getSize(); i++) {
            renamed.insert(rel.getRow(i));
        } // for 
        return renamed;
    } // rename 

    @Override
    public Relation cartesianProduct(Relation rel1, Relation rel2) {
        
        for (String attr : rel1.getAttrs()) {
            if (rel2.hasAttr(attr)) {
                throw new IllegalArgumentException("Relations have common attributes: " + attr);
            }
        }

        List<String> newAttrs = new ArrayList<>(rel1.getAttrs());
        newAttrs.addAll(rel2.getAttrs());

        List<Type> newTypes = new ArrayList<>(rel1.getTypes());
        newTypes.addAll(rel2.getTypes());

        Relation result = new RelationBuilder()
            .attributeNames(newAttrs)
            .attributeTypes(newTypes)
            .build();

        for (int i = 0; i < rel1.getSize(); i++) {
            List<Cell> row1 = rel1.getRow(i);
            for (int j = 0; j < rel2.getSize(); j++) {
                List<Cell> row2 = rel2.getRow(j);

                List<Cell> combined = new ArrayList<>(row1);
                combined.addAll(row2);

                result.insert(combined);
            }
        }
        return result;
    }

    @Override
    public Relation join(Relation rel1, Relation rel2) {
        if (rel1 == null || rel2 == null) {
            throw new NullPointerException("Relations can not be null.");
        }

        List<String> attrOne = rel1.getAttrs();
        List<String> attrTwo = rel2.getAttrs();

        List<String> common = new ArrayList<>();
        List<Integer> idOne = new ArrayList<>();
        List<Integer> idTwo = new ArrayList<>();
        for (int i = 0; i < attrOne.size(); i++) {
            String a = attrOne.get(i);
            for (int j = 0; j < attrTwo.size(); j++) {
                if (a.equals(attrTwo.get(j))) {
                    common.add(a);
                    idOne.add(i);
                    idTwo.add(j);  // <- FIX
                }
            }
        }

        // Build output
        List<String> outAttrs = new ArrayList<>(attrOne);
        for (String s : attrTwo) {
            if (!common.contains(s)) outAttrs.add(s);
        }
        List<Type> outTypes = new ArrayList<>(rel1.getTypes());
        List<Type> t2 = rel2.getTypes();
        for (int j = 0; j < attrTwo.size(); j++) {
            if (!common.contains(attrTwo.get(j))) outTypes.add(t2.get(j));
        }

        Relation out = new RelationBuilder()
                .attributeTypes(outTypes)
                .attributeNames(outAttrs)
                .build();

        // Join rows when the common attributes are equal
        for (int i = 0; i < rel1.getSize(); i++) {
            List<Cell> rA = rel1.getRow(i);
            for (int j = 0; j < rel2.getSize(); j++) {
                List<Cell> rB = rel2.getRow(j);
                boolean ok = true;
                for (int k = 0; k < common.size(); k++) {
                    if (!rA.get(idOne.get(k)).equals(rB.get(idTwo.get(k)))) {
                        ok = false; break;
                    }
                }
                if (ok) {
                    List<Cell> merged = new ArrayList<>(outAttrs.size());
                    merged.addAll(rA);
                    for (int col = 0; col < rB.size(); col++) {
                        if (!common.contains(attrTwo.get(col))) {
                            merged.add(rB.get(col));
                        }
                    }
                    out.insert(merged);
                }
            }
        }
        return out;
    }


    @Override
    public Relation join(Relation rel1, Relation rel2, Predicate p) {
        
        for (String attr : rel1.getAttrs()) {
            if (rel2.hasAttr(attr)) {
                throw new IllegalArgumentException("Relations have common attributes: " + attr);
            }
        }

        List<String> newAttrs = new ArrayList<>(rel1.getAttrs());
        newAttrs.addAll(rel2.getAttrs());

        List<Type> newTypes = new ArrayList<>(rel1.getTypes());
        newTypes.addAll(rel2.getTypes());

        Relation result = new RelationBuilder()
                     .attributeNames(newAttrs)
                     .attributeTypes(newTypes)
                     .build();

        for (int i = 0; i < rel1.getSize(); i++) {
            List<Cell> row1 = rel1.getRow(i);
            for (int j = 0; j < rel2.getSize(); j++) {
                List<Cell> row2 = rel2.getRow(j);

                List<Cell> combined = new ArrayList<>(row1);
                combined.addAll(row2);

                if (p.check(combined)) {
                    result.insert(combined);
                }
            }
        }
        return result;
    }

}
