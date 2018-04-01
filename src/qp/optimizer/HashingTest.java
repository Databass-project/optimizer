package qp.optimizer;

import java.util.HashSet;

public class HashingTest {
    public static void main(String[] args) {
        HashSet<String> setA = new HashSet<>();
        String dummy = "hello";
        setA.add("A");
        setA.add("Z");
        setA.add(dummy);
        HashSet<String> setB = new HashSet<>();
        setB.add("Z");
        setB.add("A");
        setB.add(dummy);

        if (setA.equals(setB))
            System.out.println("They are equal.");
        else
            System.out.println("They are not equal.");

        if (setA.hashCode() == setB.hashCode())
            System.out.println("They hash to the same value");
        else
            System.out.println("They don't hash to the same value");

    }

}
