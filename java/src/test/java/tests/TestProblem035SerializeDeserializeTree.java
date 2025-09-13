package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem035SerializeDeserializeTree;


public class TestProblem035SerializeDeserializeTree {
    @Test void t(){
        var r=new Problem035SerializeDeserializeTree.Node(1); r.left=new Problem035SerializeDeserializeTree.Node(2); r.right=new Problem035SerializeDeserializeTree.Node(3);
        String s=Problem035SerializeDeserializeTree.serialize(r); assertNotNull(Problem035SerializeDeserializeTree.deserialize(s));
    }
}

