package uga.csx370.mydbimpl;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import uga.csx370.mydb.*;

public class RAImpl implements RA {

    @Override
    public Relation select(Relation rel, Predicate p) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'select'");
    }

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
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'union'");
    }

    @Override
    public Relation intersect(Relation rel1, Relation rel2) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'intersect'");
    }

    @Override
    public Relation diff(Relation rel1, Relation rel2) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'diff'");
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
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'join'");
    }

}