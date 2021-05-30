import java.util.HashMap;

public class Test {
    public static void main(String[] args) {

        BinaryTree tree = new BinaryTree();

        tree.add(2);
        tree.add(27);
        tree.add(3);
        tree.add(3);
        tree.add(1);
        tree.add(5);

        tree.getFormMid();
        System.out.println("==========");
        tree.getFromMid2(tree.root);
        System.out.println("==========");


        tree.getFromPre();


    }
}
