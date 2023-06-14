package migration;

public class Test {
    public static void main(String[] args) throws InterruptedException {
        int mb = 1024 * 1024;
        // get Runtime instance
        Runtime instance = Runtime.getRuntime();
        long freeMemory = instance.freeMemory();
        long totalMemory = instance.totalMemory();
        long maxMemory = instance.maxMemory();
        System.out.println("\nFree Memory in Bytes: " + freeMemory);
        System.out.println("Free Memory in KB : " + freeMemory / 1024);
        System.out.println("Free Memory in MB : " + freeMemory / 1024 / 1024);
        System.out.println("Free Memory in GB : " + freeMemory / 1024 / 1024 / 1024);

        System.out.println("\nTotal Memory in : " + totalMemory);
        System.out.println("Total Memory in KB : " + totalMemory / 1024);
        System.out.println("Total Memory in MB : " + totalMemory / 1024 / 1024);
        System.out.println("Total Memory in GB : " + totalMemory / 1024 / 1024 / 1024);

        System.out.println("\nmax Memory in : " + maxMemory);
        System.out.println("max Memory in KB : " + maxMemory / 1024);
        System.out.println("max Memory in MB : " + maxMemory / 1024 / 1024);
        System.out.println("max Memory in GB : " + maxMemory / 1024 / 1024 / 1024);

    }
}
