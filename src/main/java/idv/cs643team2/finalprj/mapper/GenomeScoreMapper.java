/**
 * GenomeScoreMapper.java
 * 
 * Produce the total scores of each tag. The output file will allow
 * us to see which tag has the highest score - which indicate its 
 * popularity in the movie industry
 * 
 * @author Ashley Le
 */
package idv.cs643team2.finalprj.mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
 
public class GenomeScoreMapper extends Mapper<Object, Text, Text, Text> 
{
    private Text localKey = new Text();
    private Text localVal = new Text();
        
    /**
     * Map method
     */
    public void map(Object key, Text value, Mapper<Object, Text, Text, Text> .Context context) throws IOException, InterruptedException 
    {
    	String [] terms = value.toString().split(",");
        
        if(terms.length == 2)	
        	mapGenomeTags(terms, context);
        if(terms.length == 3)
        	mapGenomeScores(terms, context);
    }
    
    /**
     * Mapper for the genome-tags file
     * @param terms
     */
    private void mapGenomeTags(String [] terms, Mapper<Object, Text, Text, Text> .Context context)
    {
    	try {
    		String tagId = terms[0];
        	String tagName = terms[1];
        	
        	if(!tagId.equalsIgnoreCase("tagId"))
        	{
	        	localKey.set(tagId);
	        	
	        	if(tagName.toString().matches("[0-9]+[a-zA-Z]*."))
	        		localVal.set("removeMe_" + tagName);
	        	else
	        		localVal.set(tagName);
	        	
				context.write(localKey, localVal);
        	}
		} 
    	catch (IOException | InterruptedException e) 
    	{
			e.printStackTrace();
		}
    }
    
    /**
     * Mapper for the genome-scores file
     * @param terms
     * @param context
     */
    private void mapGenomeScores(String [] terms, Mapper<Object, Text, Text, Text> .Context context)
    {
    	try
    	{
	    	if(terms[2].matches("\\d+.*\\d*"))	// ensure relevance is double
	        {
	        	// path + tagId
	        	String tagId = terms[1].toString();
	        	String relevance = terms[2].toString();
	        	
	        	localKey.set(tagId);
	        	localVal.set(relevance);
	        	
				context.write(localKey, localVal);
	        }
	    	else
	    		return;
    	}
    	catch (IOException | InterruptedException e) 
    	{
			e.printStackTrace();
        }  
    }
}