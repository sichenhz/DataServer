import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

public class CPUMemInfo {
	 static OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
	 
	  public static void main(String []args){
		    Runtime runtime;
		            byte[] bytes;
		            
		            int cpusage=printCPUUsage();
		            System.out.println("\n \n**CPU USAGE  ** \n");
		            System.out.println (cpusage);
		          
		            System.out.println("\n \n**CPU TIME  ** \n");
			         
		            printCPUTime();
		              
		            System.out.println("\n \n**MEMORY USAGE  ** \n");
		            // Print initial memory usage.
		            runtime = Runtime.getRuntime();
		            printMemUsage(runtime);
	  }
	  
	  

	public static void printMemUsage(Runtime runtime)
    {
    long total, free, used;
    int kb = 1024;

    total = runtime.totalMemory();
    free = runtime.freeMemory();
    used = total - free;
    System.out.println("\nTotal Memory: " + total / kb + "KB");
    System.out.println(" Memory Used: " + used / kb + "KB");
    System.out.println(" Memory Free: " + free / kb + "KB");
    System.out.println("Percent Used: " + ((double)used/(double)total)*100 + "%");
    System.out.println("Percent Free: " + ((double)free/(double)total)*100 + "%");
   }
	
	public static void test()
	{
		for(int i=0;i<100;i++)
		{
			System.out.println();	
		}
		
	}
	
	   public static int printCPUUsage()
       {
           int cpuCount = operatingSystemMXBean.getAvailableProcessors();
		   long cpuStartTime = ManagementFactory.getThreadMXBean().getCurrentThreadCpuTime();

		  long elapsedStartTime = System.nanoTime();
		  test();
            long end = System.nanoTime();
            
            long totalAvailCPUTime = cpuCount * (end-elapsedStartTime);
            long totalUsedCPUTime = ManagementFactory.getThreadMXBean().getCurrentThreadCpuTime()-cpuStartTime;
          float per = ((float)totalUsedCPUTime*100)/(float)totalAvailCPUTime;
            return (int)per;
       }
	   

private static void printCPUTime() {
	ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
	for(Long threadID : threadMXBean.getAllThreadIds()) {
	    ThreadInfo info = threadMXBean.getThreadInfo(threadID);
	    System.out.println("Thread name: " + info.getThreadName());
	    System.out.println("Thread State: " + info.getThreadState());
	    System.out.println(String.format("CPU time: %s ns", 
	      threadMXBean.getThreadCpuTime(threadID)));
	  }
}
	   
}

