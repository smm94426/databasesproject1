package uga.csx370.mydbimpl;

import java.util.List;

import uga.csx370.mydb.Cell;
import uga.csx370.mydb.Type;
import uga.csx370.mydb.Predicate;


public class PredicateImpl implements Predicate {

    /**
     * EQ --- EQUAL
     * NE --- NOT EQUAL
     * GT --- GREATER THAN
     * LT --- LESS THAN
     * GE --- GREATER THAN EQUAL TO
     * LE --- LESS THAN EQUAL TO
     * CO --- CONTAINS
     * AND --- AND COMPARISON
     * OR --- OR COMPARISON
     */
    public enum Operator { EQ, NE, GT, LT, GE, LE, CO, AND, OR }

    private Cell expectedValue;
    private Integer leftColumnIndex;      
    private Integer rightColumnIndex; 
    private boolean isColumnComparison = false;

    private Operator op;

    private PredicateImpl left, right;

    public PredicateImpl(int columnIndex, Cell expectedValue, Operator op) {
        this.leftColumnIndex = columnIndex;
        this.expectedValue = expectedValue;
        this.op = op;
    }

    public PredicateImpl(int leftColumnIndex, int rightColumnIndex, Operator op) {
        if (op != Operator.EQ && op != Operator.NE && op != Operator.GT && op != Operator.LT
                && op != Operator.GE && op != Operator.LE)
            throw new IllegalArgumentException("Invalid operator for column comparison");
        this.leftColumnIndex = leftColumnIndex;
        this.rightColumnIndex = rightColumnIndex;
        this.isColumnComparison = true;
        this.op = op;

    }

    public PredicateImpl(PredicateImpl left, PredicateImpl right, Operator op) {
        if (op != Operator.AND && op != Operator.OR)
            throw new IllegalArgumentException("Operator must be AND or OR for compound predicate");
        this.left = left;
        this.right = right;
        this.op = op;
    }

    @Override
    public boolean check(List<Cell> row) {

        if (op == Operator.AND) return left.check(row) && right.check(row);
        if (op == Operator.OR)  return left.check(row) || right.check(row);

        if (isColumnComparison) {
            
            Cell leftCell = row.get(leftColumnIndex);
            Cell rightCell = row.get(rightColumnIndex);
            if (leftCell.getType() != rightCell.getType()) return false;

            return compareCells(leftCell, rightCell, op);
        }

        Cell cell = row.get(leftColumnIndex);
        if (cell.getType() != expectedValue.getType()) return false;

        return compareCells(cell, expectedValue, op);
    }

    private boolean compareCells(Cell c1, Cell c2, Operator op) {
        
        switch (op) {
            case EQ: return c1.equals(c2);
            case NE: return !c1.equals(c2);
            case GT:
                if (c1.getType() == Type.INTEGER) return c1.getAsInt() > c2.getAsInt();
                if (c1.getType() == Type.DOUBLE) return c1.getAsDouble() > c2.getAsDouble();
                return false;
            case LT:
                if (c1.getType() == Type.INTEGER) return c1.getAsInt() < c2.getAsInt();
                if (c1.getType() == Type.DOUBLE) return c1.getAsDouble() < c2.getAsDouble();
                return false;
            case GE:
                if (c1.getType() == Type.INTEGER) return c1.getAsInt() >= c2.getAsInt();
                if (c1.getType() == Type.DOUBLE) return c1.getAsDouble() >= c2.getAsDouble();
                return false;
            case LE:
                if (c1.getType() == Type.INTEGER) return c1.getAsInt() <= c2.getAsInt();
                if (c1.getType() == Type.DOUBLE) return c1.getAsDouble() <= c2.getAsDouble();
                return false;
            case CO:
                if (c1.getType() == Type.STRING) return c1.getAsString().contains(c2.getAsString());
                return false;
            default:
                return false;
        }
    }
}
