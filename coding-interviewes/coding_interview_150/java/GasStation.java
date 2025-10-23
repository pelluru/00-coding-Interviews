
/* Gas Station — greedy total+tank reset. O(n) time */
public class GasStation {
    public static int canCompleteCircuit(int[] gas, int[] cost){
        int total=0,tank=0,start=0;
        for (int i=0;i<gas.length;i++){ int d=gas[i]-cost[i]; total+=d; tank+=d; if (tank<0){ start=i+1; tank=0; } }
        return total>=0?start:-1;
    }
    public static void main(String[] args){
        System.out.println(canCompleteCircuit(new int[]{1,2,3,4,5}, new int[]{3,4,5,1,2}));
    }
}
