/**
 * GenomeScoreMapper.java
 */
package idv.cs643team2.finalprj.mapper;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import idv.cs643team2.finalprj.Main;
 
public class GenomeScoreMapper extends Mapper<Object, Text, Text, DoubleWritable> 
{
    private Text localKey = new Text();
    private DoubleWritable localVal = new DoubleWritable();
        
    /**
     * Map method
     */
    public void map(Object key, Text value, Mapper<Object, Text, Text, DoubleWritable> .Context context) throws IOException, InterruptedException 
    {
    	String filePathString = ((FileSplit) context.getInputSplit()).getPath().getName().toString();
        String [] terms = value.toString().split(",");
        
        if(terms.length == 2)	
        	mapGenomeTags(terms);
        if(terms.length == 3)
        	mapGenomeScores(terms, filePathString, context);
    }
    
    /**
     * Mapper for the genome-tags file
     * @param terms
     */
    private void mapGenomeTags(String [] terms)
    {
    	String tagId = terms[0];
    	String tagName = terms[1];
    	Main.TP.storeTagName(tagId, tagName);
    }
    
    /**
     * Mapper for the genome-scores file
     * @param terms
     * @param filePathString
     * @param context
     */
    private void mapGenomeScores(String [] terms, String filePathString, Mapper<Object, Text, Text, DoubleWritable> .Context context)
    {
    	try
    	{
	    	if(terms[2].matches("\\d+.*\\d*"))
	        {
	        	// path + tagId
	        	String tagId = terms[1].toString();
	        	String relevance = terms[2].toString();
	        	
	        	localKey.set(filePathString + "*" + tagId);
	        	localVal.set(Double.parseDouble(relevance));
	        	
					context.write(localKey, localVal);
	        }
	    	else
	    		return;
    	}
    	catch (IOException | InterruptedException e) 
    	{
			// TODO Auto-generated catch block
			e.printStackTrace();
        }  
    }
}