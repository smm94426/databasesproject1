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

    @Override
    public Relation project(Relation rel, List<String> attrs) {
        if (rel == null) throw new NullPointerException("Relation can not be null.");
        if (attrs == null) throw new NullPointerException("Projection attributes can not be null.");
        if (attrs.isEmpty()) throw new IllegalArgumentException("Projection attribute list can not be empty.");

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
        if (rel1 == null || rel2 == null) throw new NullPointerException("Relations can not be null.");

        List<String> attrs1 = rel1.getAttrs();
        List<String> attrs2 = rel2.getAttrs();

        List<String> common = new ArrayList<>();
        List<Integer> idx1 = new ArrayList<>();
        List<Integer> idx2 = new ArrayList<>();
        for (int i = 0; i < attrs1.size(); i++) {
            String a = attrs1.get(i);
            for (int j = 0; j < attrs2.size(); j++) {
                if (a.equals(attrs2.get(j))) {
                    common.add(a);
                    idx1.add(i);
                    idx2.add(j);
                }
            }
        }



        // Build output
        List<String> outAttrs = new ArrayList<>(attrs1);
        for (int j = 0; j < attrs2.size(); j++) {
            if (!common.contains(attrs2.get(j))) outAttrs.add(attrs2.get(j));
        }
        List<Type> outTypes = new ArrayList<>(rel1.getTypes());
        List<Type> t2 = rel2.getTypes();
        for (int j = 0; j < attrs2.size(); j++) {
            if (!common.contains(attrs2.get(j))) outTypes.add(t2.get(j));
        }

        Relation out = new RelationBuilder()
                .attributeTypes(outTypes)
                .attributeNames(outAttrs)
                .build();


        // join rows when the atributes are equal
        for (int i = 0; i < rel1.getSize(); i++) {
            List<Cell> rA = rel1.getRow(i);
            for (int j = 0; j < rel2.getSize(); j++) {
                List<Cell> rB = rel2.getRow(j);
                boolean ok = true;
                for (int k = 0; k < common.size(); k++) {
                    if (!rA.get(idx1.get(k)).equals(rB.get(idx2.get(k)))) {
                        ok = false; break;
                    }
                }
                if (ok) {
                    List<Cell> merged = new ArrayList<>(outAttrs.size());
                    merged.addAll(rA);
                    for (int col = 0; col < rB.size(); col++) {
                        if (!common.contains(attrs2.get(col))) {
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