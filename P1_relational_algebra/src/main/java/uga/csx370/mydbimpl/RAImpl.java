package uga.csx370.mydbimpl;

import java.util.*;

import uga.csx370.mydb.*;

public class RAImpl implements RA {

    @Override
    public Relation select(Relation rel, Predicate p) {
        
        Relation result = new RelationBuilder()
            .attributeNames(rel.getAttrs())
            .attributeTypes(rel.getTypes())
            .build();

        for (int i = 0; i < rel.getSize(); i++) {
            List<Cell> row = rel.getRow(i);
            if (p.check(row)) result.insert(row);
        }
        return result;
    }

    @Override
    public Relation project(Relation rel, List<String> attrs) {

        List<Type> types = new ArrayList<>();
        for (String attr : attrs) {
            int colIndex = rel.getAttrIndex(attr);
            types.add(rel.getTypes().get(colIndex));
        }

        Relation result = new RelationBuilder()
            .attributeNames(attrs)
            .attributeTypes(types)
            .build();        

        for (int i = 0; i < rel.getSize(); i++) {
            List<Cell> row = rel.getRow(i);
            List<Cell> newRow = new ArrayList<>();

            for (String attr : attrs) {
                int colIndex = rel.getAttrIndex(attr);
                newRow.add(row.get(colIndex));
            } 
            result.insert(newRow);
        }
        return result;
    }

    @Override
    public Relation union(Relation rel1, Relation rel2) {

        if (!rel1.getAttrs().equals(rel2.getAttrs()) || !rel1.getTypes().equals(rel2.getTypes())) {
            throw new IllegalArgumentException("Relations are not union compatible.");
        }
    
        Relation result = new RelationBuilder()
            .attributeNames(rel1.getAttrs())
            .attributeTypes(rel1.getTypes())
            .build();

        Set<List<Cell>> seen = new HashSet<>();

        for (int i = 0; i < rel1.getSize(); i++) {
            List<Cell> row = rel1.getRow(i);
            if (seen.add(row)) result.insert(row);
        }

        for (int i = 0; i < rel2.getSize(); i++) {
            List<Cell> row = rel2.getRow(i);
            if (seen.add(row)) result.insert(row);
        }
        return result;
    }

    @Override
    public Relation intersect(Relation rel1, Relation rel2) {

        if (!rel1.getAttrs().equals(rel2.getAttrs()) || !rel1.getTypes().equals(rel2.getTypes())) {
            throw new IllegalArgumentException("Relations are not union compatible.");
        }

        Relation result = new RelationBuilder()
            .attributeNames(rel1.getAttrs())
            .attributeTypes(rel1.getTypes())
            .build();

        Set<List<Cell>> set1 = new HashSet<>();

        for (int i = 0; i < rel1.getSize(); i++) set1.add(rel1.getRow(i));

        for (int i = 0; i < rel2.getSize(); i++) {
            List<Cell> row = rel2.getRow(i);
            if (set1.contains(row)) result.insert(row);
        }
        return result;
    }

    @Override
    public Relation diff(Relation rel1, Relation rel2) {
        
        if (!rel1.getAttrs().equals(rel2.getAttrs()) || !rel1.getTypes().equals(rel2.getTypes())) {
        throw new IllegalArgumentException("Relations are not union compatible.");
        }

        Relation result = new RelationBuilder()
            .attributeNames(rel1.getAttrs())
            .attributeTypes(rel1.getTypes())
            .build();
    
        Set<List<Cell>> set2 = new HashSet<>();
        for (int i = 0; i < rel2.getSize(); i++) set2.add(rel2.getRow(i));

        for (int i = 0; i < rel1.getSize(); i++) {
            List<Cell> row = rel1.getRow(i);
            if (!set2.contains(row)) result.insert(row);
        }
        return result;
    }

    @Override
    public Relation rename(Relation rel, List<String> origAttr, List<String> renamedAttr) {
        
        if (origAttr.size() != renamedAttr.size()) {
            throw new IllegalArgumentException("Mismatch in number of attributes");
        }

        for (String attr : origAttr) {
            if (!rel.hasAttr(attr)) {
                throw new IllegalArgumentException("Attribute " + attr + " does not exist");
            }
        }

        Set<String> renamedSet = new HashSet<>(renamedAttr);

        if (renamedSet.size() != renamedAttr.size()) {
            throw new IllegalArgumentException("Renamed attributes must be unique");
        }

        List<String> oldAttrs = rel.getAttrs();
        List<Type> types = rel.getTypes();

        List<String> newAttrs = new ArrayList<>(oldAttrs);

        for (int i = 0; i < origAttr.size(); i++) {
            int index = rel.getAttrIndex(origAttr.get(i));
            newAttrs.set(index, renamedAttr.get(i));
        }

        Relation result = new RelationBuilder()
            .attributeNames(newAttrs)
            .attributeTypes(types)
            .build();
        
        for (int i = 0; i < rel.getSize(); i++) result.insert(rel.getRow(i));

        return result;
    }

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
        
        List<String> attrs1 = rel1.getAttrs();
        List<String> attrs2 = rel2.getAttrs();

        List<Integer> rel1CommonIndex = new ArrayList<>();
        List<Integer> rel2CommonIndex = new ArrayList<>();

        for (int i = 0; i < attrs1.size(); i++) {
            String attr = attrs1.get(i);
            if (attrs2.contains(attr)) {
                rel1CommonIndex.add(i);
                rel2CommonIndex.add(rel2.getAttrIndex(attr));
            }
        }

        List<String> newAttrs = new ArrayList<>(attrs1);
        List<Type> newTypes = new ArrayList<>(rel1.getTypes());

        for (int i = 0; i < attrs2.size(); i++) {
            if (!attrs1.contains(attrs2.get(i))) {
                newAttrs.add(attrs2.get(i));
                newTypes.add(rel2.getTypes().get(i));
            }
        }

        Relation result = new RelationBuilder()
            .attributeNames(newAttrs)
            .attributeTypes(newTypes)
            .build();

        for (int i = 0; i < rel1.getSize(); i++) {
            List<Cell> row1 = rel1.getRow(i);
            for (int j = 0; j < rel2.getSize(); j++) {
                List<Cell> row2 = rel2.getRow(j);

                boolean match = true;
                for (int k = 0; k < rel1CommonIndex.size(); k++) {
                    if (!row1.get(rel1CommonIndex.get(k))
                        .equals(row2.get(rel2CommonIndex.get(k)))) {
                        match = false;
                        break;
                    }
                }

                if (match) {
                    List<Cell> combined = new ArrayList<>(row1);
                        for (int k = 0; k < row2.size(); k++) {
                        if (!rel2CommonIndex.contains(k)) {
                            combined.add(row2.get(k));
                       }
                    }
                    result.insert(combined);
                }
            }
        }
        return result;
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