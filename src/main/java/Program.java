public class Program {

    public static void main(String []args){

        try{

            //SparkActionExample simpleSparkExample = new SparkActionExample();
            //simpleSparkExample.simpleArrayAction();

            //SparkTransformExample sparkTransformExample = new SparkTransformExample();
            //sparkTransformExample.sparkTransformExample();

            SparkDataFrame sparkDataFrame = new SparkDataFrame();
            sparkDataFrame.sparkDataRead();

            System.out.println("End of main program");
        } catch(Exception ex){
            System.out.println("Error " + ex.getMessage());
        }

    }

}
