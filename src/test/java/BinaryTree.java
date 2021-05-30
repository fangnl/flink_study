import org.apache.calcite.util.Static;

import java.util.Stack;

public class BinaryTree {
    Node root;

    public class Node {
        int value;
        Node left;
        Node right;

        public Node(int value, Node left, Node right) {
            this.value = value;
            this.left = left;
            this.right = right;
        }

        public Node(int value) {
            this.value = value;
        }

        public Node() {
        }
    }

    public BinaryTree() {
        this.root = null;
    }


    public void add(Node fNode, int value) {
        if (value > fNode.value && fNode.right == null)
            fNode.right = new Node(value);
        else if (value > fNode.value)
            add(root.right, value);
        else if (fNode.left == null) {
            fNode.left = new Node(value);
        } else {
            add(fNode.left, value);
        }

    }

    public void add(int value) {
        if (root == null)
            root = new Node(value);
        else
            add(root, value);
    }


    //mid
    public void getFormMid() {
        Stack<Node> stack = new Stack<>();
//        stack.push(root);
        Node header = root;

        while (!stack.isEmpty() || header != null) {
            if (header != null) {
                //先把左边放进栈
                stack.push(header);
                header = header.left;
            } else {
                //如果左边没有了处理右边
                Node pop = stack.pop();
                System.out.print(pop.value + ",");
                header = pop.right;
            }
        }
    }

    //递归
    public void getFromMid2(Node node) {
        if (node == null) return;
        if (node.left != null)
            getFromMid2(node.left);
        System.out.println(node.value);
        if (node.right != null)
            getFromMid2(node.right);
    }

    public void getFromPre() {
        Stack<Node> stack = new Stack<>();
        stack.push(root);
        while (!stack.isEmpty()) {
            Node pop = stack.pop();
            System.out.println(pop.value);
            if (pop.right != null)
                stack.push(pop.right);
            if (pop.left != null)
                stack.push(pop.left);
        }
    }


}
