/* simple buffer manager that distributes the buffers equally among all the join operators */
package qp.optimizer;

public class BufferManager {

    public static int numBuffer;
    public static int numJoin;
    public static int buffPerJoin;

    public BufferManager(int numBuffer, int numJoinOrOrderBy) {
        this.numBuffer = numBuffer;
        this.numJoin = numJoin;
        buffPerJoin = numBuffer / numJoinOrOrderBy;
    }

    public static int getBuffersPerJoin() {
        return buffPerJoin;
    }
    
    public static int getBuffers(){
    	return numBuffer;
    }
}
