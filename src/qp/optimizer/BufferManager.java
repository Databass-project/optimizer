/* simple buffer manager that distributes the buffers equally among all the join operators */
package qp.optimizer;

public class BufferManager {

    public static int numBuffer;
    public static int numJoin;
    public static int buffPerJoin;

    public BufferManager(int numBuffer, int numJoin) {
        this.numBuffer = numBuffer;
        this.numJoin = numJoin;
        buffPerJoin = numBuffer / numJoin;
    }

    public static int getBuffersPerJoin() {
        return buffPerJoin;
    }
}
