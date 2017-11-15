package com.monkeyboy.java.hadoop.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
/**
 * 主要是针对于getRecordWriter Synchronize方法的修改把锁的粒度改小
 * @author monkeyboy
 * 2017年7月20日下午6:08:51
 *
 * @param <KEYOUT>
 * @param <VALUEOUT>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class MultipleOutputs<KEYOUT, VALUEOUT> {

  private static final String MULTIPLE_OUTPUTS = "mapreduce.multipleoutputs";

  private static final String MO_PREFIX = 
    "mapreduce.multipleoutputs.namedOutput.";

  private static final String FORMAT = ".format";
  private static final String KEY = ".key";
  private static final String VALUE = ".value";
  private static final String COUNTERS_ENABLED = 
    "mapreduce.multipleoutputs.counters";

  /**
   * Counters group used by the counters of MultipleOutputs.
   */
  private static final String COUNTERS_GROUP = MultipleOutputs.class.getName();

  /**
   * Cache for the taskContexts
   */
  private Map<String, TaskAttemptContext> taskContexts = new HashMap<String, TaskAttemptContext>();
  /**
   * Cached TaskAttemptContext which uses the job's configured settings
   */
  private TaskAttemptContext jobOutputFormatContext;

  /**
   * Checks if a named output name is valid token.
   *
   * @param namedOutput named output Name
   * @throws IllegalArgumentException if the output name is not valid.
   */
  private static void checkTokenName(String namedOutput) {
    if (namedOutput == null || namedOutput.length() == 0) {
      throw new IllegalArgumentException(
        "Name cannot be NULL or emtpy");
    }
    for (char ch : namedOutput.toCharArray()) {
      if ((ch >= 'A') && (ch <= 'Z')) {
        continue;
      }
      if ((ch >= 'a') && (ch <= 'z')) {
        continue;
      }
      if ((ch >= '0') && (ch <= '9')) {
        continue;
      }
      throw new IllegalArgumentException(
        "Name cannot be have a '" + ch + "' char");
    }
  }

  /**
   * Checks if output name is valid.
   *
   * name cannot be the name used for the default output
   * @param outputPath base output Name
   * @throws IllegalArgumentException if the output name is not valid.
   */
  private static void checkBaseOutputPath(String outputPath) {
    if (outputPath.equals(FileOutputFormat.PART)) {
      throw new IllegalArgumentException("output name cannot be 'part'");
    }
  }
  
  /**
   * Checks if a named output name is valid.
   *
   * @param namedOutput named output Name
   * @throws IllegalArgumentException if the output name is not valid.
   */
  private static void checkNamedOutputName(JobContext job,
      String namedOutput, boolean alreadyDefined) {
    checkTokenName(namedOutput);
    checkBaseOutputPath(namedOutput);
    List<String> definedChannels = getNamedOutputsList(job);
    if (alreadyDefined && definedChannels.contains(namedOutput)) {
      throw new IllegalArgumentException("Named output '" + namedOutput +
        "' already alreadyDefined");
    } else if (!alreadyDefined && !definedChannels.contains(namedOutput)) {
      throw new IllegalArgumentException("Named output '" + namedOutput +
        "' not defined");
    }
  }

  // Returns list of channel names.
  private static List<String> getNamedOutputsList(JobContext job) {
    List<String> names = new ArrayList<String>();
    StringTokenizer st = new StringTokenizer(
      job.getConfiguration().get(MULTIPLE_OUTPUTS, ""), " ");
    while (st.hasMoreTokens()) {
      names.add(st.nextToken());
    }
    return names;
  }

  // Returns the named output OutputFormat.
  @SuppressWarnings("unchecked")
  private static Class<? extends OutputFormat<?, ?>> getNamedOutputFormatClass(
    JobContext job, String namedOutput) {
    return (Class<? extends OutputFormat<?, ?>>)
      job.getConfiguration().getClass(MO_PREFIX + namedOutput + FORMAT, null,
      OutputFormat.class);
  }

  // Returns the key class for a named output.
  private static Class<?> getNamedOutputKeyClass(JobContext job,
                                                String namedOutput) {
    return job.getConfiguration().getClass(MO_PREFIX + namedOutput + KEY, null,
      Object.class);
  }

  // Returns the value class for a named output.
  private static Class<?> getNamedOutputValueClass(
      JobContext job, String namedOutput) {
    return job.getConfiguration().getClass(MO_PREFIX + namedOutput + VALUE,
      null, Object.class);
  }

  /**
   * Adds a named output for the job.
   * <p/>
   *
   * @param job               job to add the named output
   * @param namedOutput       named output name, it has to be a word, letters
   *                          and numbers only, cannot be the word 'part' as
   *                          that is reserved for the default output.
   * @param outputFormatClass OutputFormat class.
   * @param keyClass          key class
   * @param valueClass        value class
   */
  @SuppressWarnings("unchecked")
  public static void addNamedOutput(Job job, String namedOutput,
      Class<? extends OutputFormat> outputFormatClass,
      Class<?> keyClass, Class<?> valueClass) {
    checkNamedOutputName(job, namedOutput, true);
    Configuration conf = job.getConfiguration();
    conf.set(MULTIPLE_OUTPUTS,
      conf.get(MULTIPLE_OUTPUTS, "") + " " + namedOutput);
    conf.setClass(MO_PREFIX + namedOutput + FORMAT, outputFormatClass,
      OutputFormat.class);
    conf.setClass(MO_PREFIX + namedOutput + KEY, keyClass, Object.class);
    conf.setClass(MO_PREFIX + namedOutput + VALUE, valueClass, Object.class);
  }

  /**
   * Enables or disables counters for the named outputs.
   * 
   * The counters group is the {@link MultipleOutputs} class name.
   * The names of the counters are the same as the named outputs. These
   * counters count the number records written to each output name.
   * By default these counters are disabled.
   *
   * @param job    job  to enable counters
   * @param enabled indicates if the counters will be enabled or not.
   */
  public static void setCountersEnabled(Job job, boolean enabled) {
    job.getConfiguration().setBoolean(COUNTERS_ENABLED, enabled);
  }

  /**
   * Returns if the counters for the named outputs are enabled or not.
   * By default these counters are disabled.
   *
   * @param job    the job 
   * @return TRUE if the counters are enabled, FALSE if they are disabled.
   */
  public static boolean getCountersEnabled(JobContext job) {
    return job.getConfiguration().getBoolean(COUNTERS_ENABLED, false);
  }

  /**
   * Wraps RecordWriter to increment counters. 
   */
  @SuppressWarnings("unchecked")
  private static class RecordWriterWithCounter extends RecordWriter {
    private RecordWriter writer;
    private String counterName;
    private TaskInputOutputContext context;

    public RecordWriterWithCounter(RecordWriter writer, String counterName,
                                   TaskInputOutputContext context) {
      this.writer = writer;
      this.counterName = counterName;
      this.context = context;
    }

    @SuppressWarnings({"unchecked"})
    public void write(Object key, Object value) 
        throws IOException, InterruptedException {
      context.getCounter(COUNTERS_GROUP, counterName).increment(1);
      writer.write(key, value);
    }

    public void close(TaskAttemptContext context) 
        throws IOException, InterruptedException {
      writer.close(context);
    }
  }

  // instance code, to be used from Mapper/Reducer code

  private TaskInputOutputContext<?, ?, KEYOUT, VALUEOUT> context;
  private Set<String> namedOutputs;
  private Map<String, RecordWriter<?, ?>> recordWriters;
  private boolean countersEnabled;
  
  /**
   * Creates and initializes multiple outputs support,
   * it should be instantiated in the Mapper/Reducer setup method.
   *
   * @param context the TaskInputOutputContext object
   */
  public MultipleOutputs(
      TaskInputOutputContext<?, ?, KEYOUT, VALUEOUT> context) {
    this.context = context;
    namedOutputs = Collections.unmodifiableSet(
      new HashSet<String>(MultipleOutputs.getNamedOutputsList(context)));
    recordWriters = new HashMap<String, RecordWriter<?, ?>>();
    countersEnabled = getCountersEnabled(context);
  }

  /**
   * Write key and value to the namedOutput.
   *
   * Output path is a unique file generated for the namedOutput.
   * For example, {namedOutput}-(m|r)-{part-number}
   * 
   * @param namedOutput the named output name
   * @param key         the key
   * @param value       the value
   */
  @SuppressWarnings("unchecked")
  public <K, V> void write(String namedOutput, K key, V value)
      throws IOException, InterruptedException {
    write(namedOutput, key, value, namedOutput);
  }

  /**
   * Write key and value to baseOutputPath using the namedOutput.
   * 
   * @param namedOutput    the named output name
   * @param key            the key
   * @param value          the value
   * @param baseOutputPath base-output path to write the record to.
   * Note: Framework will generate unique filename for the baseOutputPath
   */
  @SuppressWarnings("unchecked")
  public <K, V> void write(String namedOutput, K key, V value,
      String baseOutputPath) throws IOException, InterruptedException {
    checkNamedOutputName(context, namedOutput, false);
    checkBaseOutputPath(baseOutputPath);
    if (!namedOutputs.contains(namedOutput)) {
      throw new IllegalArgumentException("Undefined named output '" +
        namedOutput + "'");
    }
    TaskAttemptContext taskContext = getContext(namedOutput);
    getRecordWriter(taskContext, baseOutputPath).write(key, value);
  }

  /**
   * Write key value to an output file name.
   * 
   * Gets the record writer from job's output format.  
   * Job's output format should be a FileOutputFormat.
   * 
   * @param key       the key
   * @param value     the value
   * @param baseOutputPath base-output path to write the record to.
   * Note: Framework will generate unique filename for the baseOutputPath
   */
  @SuppressWarnings("unchecked")
  public void write(KEYOUT key, VALUEOUT value, String baseOutputPath) 
      throws IOException, InterruptedException {
    checkBaseOutputPath(baseOutputPath);
    if (jobOutputFormatContext == null) {
      jobOutputFormatContext = 
        new TaskAttemptContextImpl(context.getConfiguration(), 
                                   context.getTaskAttemptID(),
                                   new WrappedStatusReporter(context));
    }
    getRecordWriter(jobOutputFormatContext, baseOutputPath).write(key, value);
  }

  /**
   * 把基于方法的synchronized改成了双重锁
   * @param taskContext
   * @param baseFileName
   * @return
   * @throws IOException
   * @throws InterruptedException
   */
  // by being synchronized MultipleOutputTask can be use with a
  // MultithreadedMapper.
  @SuppressWarnings("unchecked")
  private RecordWriter getRecordWriter(
      TaskAttemptContext taskContext, String baseFileName) 
      throws IOException, InterruptedException {
    
    // look for record-writer in the cache
    RecordWriter writer = recordWriters.get(baseFileName);
    
    // If not in cache, create a new one
    if (writer == null) {
    	synchronized(this) {
    		if (null==writer)
    		{
    			// get the record writer from context output format
      	      FileOutputFormat.setOutputName(taskContext, baseFileName);
      	      try {
      	        writer = ((OutputFormat) ReflectionUtils.newInstance(
      	          taskContext.getOutputFormatClass(), taskContext.getConfiguration()))
      	          .getRecordWriter(taskContext);
      	      } catch (ClassNotFoundException e) {
      	        throw new IOException(e);
      	      }
      	 
      	      // if counters are enabled, wrap the writer with context 
      	      // to increment counters 
      	      if (countersEnabled) {
      	        writer = new RecordWriterWithCounter(writer, baseFileName, context);
      	      }
      	      
      	      // add the record-writer to the cache
      	      recordWriters.put(baseFileName, writer);
    		}
    		
		}
      
    }
    return writer;
  }

   // Create a taskAttemptContext for the named output with 
   // output format and output key/value types put in the context
  private TaskAttemptContext getContext(String nameOutput) throws IOException {
      
    TaskAttemptContext taskContext = taskContexts.get(nameOutput);
    
    if (taskContext != null) {
        return taskContext;
    }
    
    // The following trick leverages the instantiation of a record writer via
    // the job thus supporting arbitrary output formats.
    Job job = new Job(context.getConfiguration());
    job.setOutputFormatClass(getNamedOutputFormatClass(context, nameOutput));
    job.setOutputKeyClass(getNamedOutputKeyClass(context, nameOutput));
    job.setOutputValueClass(getNamedOutputValueClass(context, nameOutput));
    taskContext = new TaskAttemptContextImpl(job.getConfiguration(), context
        .getTaskAttemptID(), new WrappedStatusReporter(context));

    taskContexts.put(nameOutput, taskContext);

    return taskContext;
  }

  private static class WrappedStatusReporter extends StatusReporter {

    TaskAttemptContext context;

    public WrappedStatusReporter(TaskAttemptContext context) {
      this.context = context;
    }

    @Override
    public Counter getCounter(Enum<?> name) {
      return context.getCounter(name);
    }

    @Override
    public Counter getCounter(String group, String name) {
      return context.getCounter(group, name);
    }

    @Override
    public void progress() {
      context.progress();
    }

    @Override
    public float getProgress() {
      return context.getProgress();
    }
    
    @Override
    public void setStatus(String status) {
      context.setStatus(status);
    }
  }

  /**
   * Closes all the opened outputs.
   * 
   * This should be called from cleanup method of map/reduce task.
   * If overridden subclasses must invoke <code>super.close()</code> at the
   * end of their <code>close()</code>
   * 
   */
  @SuppressWarnings("unchecked")
  public void close() throws IOException, InterruptedException {
    for (RecordWriter writer : recordWriters.values()) {
      writer.close(context);
    }
  }
}
