package adpushup.logs;

import sun.rmi.runtime.Log;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;

public class LogWriter {

    private static final String SUMMARY_ALL="summary_all.txt";
    private static final String SUMMARY_SUCCESS="summary_success.txt";
    private static final String SUMMARY_FAILED="summary_failed.txt";
    private static final String ERROR_LOG="error_log.txt";
    private static final String NEW_LINE="\n";



    private static volatile LogWriter writer;
    private FileWriter summaryAll;
    private FileWriter summarySuccess;
    private FileWriter summaryFailed;
    private FileWriter errorLog;


    //Not handled for multiple threads as called by one thread in this case
    public static LogWriter getInstance(){
        if(writer==null){
            writer=new LogWriter();
            try {
                writer.init();
            } catch (IOException e) {
                System.out.println("Unable to instantiate logger");
            }
        }
        return writer;
    }

    public void init() throws IOException {
        recreate();
        summaryAll=new FileWriter(SUMMARY_ALL,true);
        summarySuccess=new FileWriter(SUMMARY_SUCCESS,true);
        summaryFailed=new FileWriter(SUMMARY_FAILED,true);
        errorLog=new FileWriter(ERROR_LOG,true);
    }

    public void recreate() throws IOException {
        deleteAndCreate(new File(SUMMARY_SUCCESS));
        deleteAndCreate(new File(SUMMARY_FAILED));
        deleteAndCreate(new File(SUMMARY_ALL));
        deleteAndCreate(new File(ERROR_LOG));
    }

    public void deleteAndCreate(File file) throws IOException {
        if(file.exists()) file.delete();
        file.createNewFile();
    }

    public static void SUCCESS(String msg){
        try {
                getInstance().summarySuccess.append(msg+NEW_LINE);
        } catch (IOException e) {}
    }

    public static void FAILURE(String msg){
        try {
                getInstance().summaryFailed.append(msg+NEW_LINE);
        }catch (IOException e) {}
    }

    public static void ERROR(String msg) {
        try {
                getInstance().errorLog.append(msg+NEW_LINE);
        }catch(IOException e){ }
    }

    public static void SUMMARY(String msg){
        try {
                getInstance().summaryAll.append(msg+NEW_LINE);
        }catch (IOException e) {}
    }

}
