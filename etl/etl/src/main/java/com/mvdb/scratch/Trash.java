package com.mvdb.scratch;

public class Trash
{


 // Order order1 = new Order(orderDAO.getNextSequenceValue(),
 // RandomUtil.getRandomString(5),RandomUtil.getRandomInt(), new
 // Date(tm-10000000000L), new Date(tm-5000000000L));
 // Order order3 = new Order(orderDAO.getNextSequenceValue(),
 // RandomUtil.getRandomString(5),RandomUtil.getRandomInt(), new
 // Date(tm-20000000000L), new Date(tm-4000000000L));
 // Order order2 = new Order(orderDAO.getNextSequenceValue(),
 // RandomUtil.getRandomString(5),RandomUtil.getRandomInt(), new
 // Date(tm-30000000000L), new Date(tm-6000000000L));
 // orders.add(order1);
 // orders.add(order2);
 // orders.add(order3);

 /**
  * CREATE TABLE orders ( ORDER_ID bigint NOT NULL, NOTE varchar(100) NOT NULL,
  * SALE_CODE int NOT NULL, CREATE_TIME timestamp NOT NULL, UPDATE_TIME timestamp
  * NOT NULL ); COMMIT;
  * 
  * CREATE SEQUENCE com_etl_good_bad_Order START 101; commit; SELECT
  * nextval('com_etl_good_bad_Order');
  */

 /**
  * Order orderA = orderDAO.findByOrderId(1); System.out.println("Order A : " +
  * orderA);
  * 
  * 
  * 
  * List<Order> orderAs = orderDAO.findAll(); for(Order order: orderAs){
  * System.out.println("Order As : " + order); }
  **/
    
    /*
     * 
     * FSDataInputStream in = fs1.open(inFile); FSDataOutputStream out =
     * fs2.create(outFile); System.out.println("Copy " + inFile.toString() +
     * " to " + outFile.toString());
     * 
     * int bytesRead = -1; byte[] buffer = new byte[1024]; while ((bytesRead
     * = in.read(buffer)) > 0) { out.write(buffer, 0, bytesRead); }
     * 
     * in.close(); out.close();
   */
    
    
    /*
     
    private static void testHdfs(String infileName, String outFileName) throws IOException
   {
       org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
       conf.addResource(new Path("/home/umesh/ops/hadoop-1.2.0/conf/core-site.xml")); 
       FileSystem fs2 = FileSystem.get(conf);
       FileSystem fs1 = FileSystem.get(new org.apache.hadoop.conf.Configuration());

       Path inFile = new Path(infileName);
       Path outFile = new Path(outFileName);
       
       if (fs2.exists(outFile))
       {
           boolean deleteSuccess = fs2.delete(outFile, true);
           if(deleteSuccess == false)
           {
               printAndExit("Unable to delete " + outFile.toString()); 
           }
       }
       if (!fs1.exists(inFile))
         printAndExit("Input file not found");
       FileStatus fileStatus1 = fs1.getFileStatus(inFile);        
       if (!fileStatus1.isDir())
         printAndExit("Input should be a directory");
       if (fs2.exists(outFile))
         printAndExit("Output already exists");

       System.out.println("Copy " + inFile.toString() + " to " + outFile.toString());
       FileUtil.copy(fs1, inFile, fs2, outFile, false, conf);
       
      
       
//       FSDataInputStream in = fs1.open(inFile);
//       FSDataOutputStream out = fs2.create(outFile);
//       System.out.println("Copy " + inFile.toString() + " to " + outFile.toString());
//       
//       int bytesRead = -1;
//       byte[] buffer = new byte[1024];
//       while ((bytesRead = in.read(buffer)) > 0) {
//         out.write(buffer, 0, bytesRead);
//       }
//
//       in.close();
//       out.close();
       
       System.exit(1);
   }
   
   
   private static void printAndExit(String string)
   {
       System.out.println(string);
       System.exit(1);        
   }
   
*/

}
