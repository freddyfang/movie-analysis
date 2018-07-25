/**
 * TagProcessor.java
 * 
 * This class perform post-processing functions that helps
 * 		sorting and writing the sorted tags to a file
 */
package idv.cs643team2.finalprj.helpers;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import java.util.Map.Entry;

public class TagProcessor 
{ 
	private String OUTPUT_DIR = "";
    private Map <String, Double> unsortedTags = new HashMap <String, Double> ();	// contains all of the tags
    private Map <String, String> tagsName = new HashMap <String, String> ();
    List<String> records = new ArrayList<String>();
    
    public TagProcessor()
    {
    	unsortedTags = new HashMap <String, Double> ();	
    }
    
    /**
     * Add a movie to the map
     * @param movie the movie 
     */
    public void collectTags(String tId, Double relevance)
    {
    	unsortedTags.put(tId, relevance);
    }
    
    public void storeTagName(String tId, String tName)
    {
    	tagsName.put(tId, tName);
    }
    
    /**
     * Sort all the tags based on popularity
     * @param unsorted the unsorted tags
     * @return map that contains sorted tags (according to popularity)
     */
    public Map<String, Double> sortTags(Map <String, Double> unsorted)
    {
    	return sortByValue(unsorted);
    }
    
    /**
     * Write all tags to file (sorted by popularity)
     */
    public void writeSortedTags()
    {	
    	records = new ArrayList<String>();
    	records.add("tagId, tag, score");
    	
    	for(Map.Entry<String, Double> entry : sortTags(unsortedTags).entrySet())
    		records.add(entry.getKey() + ", " + tagsName.get(entry.getKey()) + ", " +  entry.getValue() + "\n");
		
    	writeRaw(records, "genome-scores-sorted-tags.csv");		
    }
    
    /**
     * Sort map
     * @param map
     * @return sorted map
     */
    public <K, V extends Comparable< ? super V>> Map<K, V> sortByValue (Map<K, V> map) 
    {
        List<Entry<K, V>> list = new ArrayList<>(map.entrySet());
        list.sort(Entry.comparingByValue());

        Map<K, V> result = new LinkedHashMap<>();
        for(int index = list.size()-1; index >= 0; index --)
        {
            Entry <K, V> entry = list.get(index);
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }
    
    /**
     * Sort map
     * @param map
     * @return sorted map
     */
    public Map<String, Integer> sortByKey (Map<String, Integer> map) 
    {
        return new TreeMap<String, Integer> (map);
    }
    
    /**
     * Write records to designated file
     * @param records
     * @param fileName
     */
    private void writeRaw(List<String> records, String fileName)
    {
    	try 
    	{
        	File file = new File(OUTPUT_DIR + "/" + fileName);
        	if(file.exists())
        		file.delete();
        	
        	file.createNewFile();
        
            FileWriter writer = new FileWriter(file);
            write(records, writer);
        } 
        catch (IOException e)
        {
        	e.printStackTrace();
        }
    }
    
    /**
     * Write all records to a file
     * @param records content to be written
     * @param writer the writer
     * @throws IOException 
     */
    private void write(List<String> records, Writer writer) throws IOException {
        //long start = System.currentTimeMillis();
        for (String record: records) 
        	writer.write(record);

        writer.flush();
        writer.close();
        //long end = System.currentTimeMillis();
        //System.out.println((end - start) / 1000f + " seconds");
    }
    
    /**
     * Set the output path (post processing)
     * @param path the directory
     */
    public void setOutputPath(String path)
    {
    	this.OUTPUT_DIR = path;
    }
}