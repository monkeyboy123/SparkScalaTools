package com.monkeyboy.java.hadoop.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapred.LocatedFileStatusFetcher;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

/**
 * 防止spark查找match文件的时候没有找到文件报异常
 * 
 * @author monkeyboy 2017年6月27日上午11:21:21
 */
public class EmptiableTextInputFormat extends TextInputFormat
{
	private static final double SPLIT_SLOP = 1.1;
	
	private List<FileStatus> singleThreadedListStatus(JobContext job,
			Path[] dirs,
			PathFilter inputFilter, boolean recursive) throws IOException
	{
		List<FileStatus> result = new ArrayList<FileStatus>();
		List<IOException> errors = new ArrayList<IOException>();
		for (int i = 0; i < dirs.length; ++i)
		{
			Path p = dirs[i];
			FileSystem fs = p.getFileSystem(job.getConfiguration());
			FileStatus[] matches = fs.globStatus(p, inputFilter);
			if (matches == null)
			{
				errors.add(new IOException("Input path does not exist: " + p));
			}
		   //注释这里if防止多个路径 有一个路径抛异常
			else if (matches.length == 0)
			{
				continue;
//				errors.add(new IOException("Input Pattern " + p
//						+ " matches 0 files"));
			}
			else
			{
				for (FileStatus globStat : matches)
				{
					if (globStat.isDirectory())
					{
						RemoteIterator<LocatedFileStatus> iter =
								fs.listLocatedStatus(globStat.getPath());
						while (iter.hasNext())
						{
							LocatedFileStatus stat = iter.next();
							if (inputFilter.accept(stat.getPath()))
							{
								if (recursive && stat.isDirectory())
								{
									addInputPathRecursively(result, fs,
											stat.getPath(),
											inputFilter);
								}
								else
								{
									result.add(stat);
								}
							}
						}
					}
					else
					{
						result.add(globStat);
					}
				}
			}
		}
		if (!errors.isEmpty())
		{
			throw new InvalidInputException(errors);
		}
		return result;
	}
	
	private static class MultiPathFilter implements PathFilter
	{
		private List<PathFilter> filters;
		
		public MultiPathFilter(List<PathFilter> filters)
		{
			this.filters = filters;
		}
		
		public boolean accept(Path path)
		{
			for (PathFilter filter : filters)
			{
				if (!filter.accept(path))
				{
					return false;
				}
			}
			return true;
		}
	}
	
	private static final PathFilter hiddenFileFilter = new PathFilter()
	{
		public boolean accept(Path p)
		{
			String name = p.getName();
			return !name.startsWith("_") && !name.startsWith(".");
		}
	};
	
	protected List<FileStatus> listStatus(JobContext job
			) throws IOException
	{
		Path[] dirs = getInputPaths(job);
		if (dirs.length == 0)
		{
			throw new IOException("No input paths specified in job");
		}
		// get tokens for all the required FileSystems..
		TokenCache.obtainTokensForNamenodes(job.getCredentials(), dirs,
				job.getConfiguration());
		// Whether we need to recursive look into the directory structure
		boolean recursive = getInputDirRecursive(job);
		// creates a MultiPathFilter with the hiddenFileFilter and the
		// user provided one (if any).
		List<PathFilter> filters = new ArrayList<PathFilter>();
		filters.add(hiddenFileFilter);
		PathFilter jobFilter = getInputPathFilter(job);
		if (jobFilter != null)
		{
			filters.add(jobFilter);
		}
		PathFilter inputFilter = new MultiPathFilter(filters);
		List<FileStatus> result = null;
		int numThreads = job.getConfiguration().getInt(LIST_STATUS_NUM_THREADS,
				DEFAULT_LIST_STATUS_NUM_THREADS);
		Stopwatch sw = new Stopwatch().start();
		if (numThreads == 1)
		{
			result = singleThreadedListStatus(job, dirs, inputFilter, recursive);
		}
		else
		{
			Iterable<FileStatus> locatedFiles = null;
			try
			{
				LocatedFileStatusFetcher locatedFileStatusFetcher = new LocatedFileStatusFetcher(
						job.getConfiguration(), dirs, recursive, inputFilter,
						true);
				locatedFiles = locatedFileStatusFetcher.getFileStatuses();
			}
			catch (InterruptedException e)
			{
				throw new IOException("Interrupted while getting file statuses");
			}
			result = Lists.newArrayList(locatedFiles);
		}
		sw.stop();
		return result;
	}
	
	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException
	{
		// try
		// {
		// return super.getSplits(job);
		// }
		// catch (InvalidInputException e)
		// {
		// return Collections.emptyList();
		// }
		Stopwatch sw = new Stopwatch().start();
		long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
		long maxSize = getMaxSplitSize(job);
		// generate splits
		List<InputSplit> splits = new ArrayList<InputSplit>();
		List<FileStatus> files = listStatus(job);
		for (FileStatus file : files)
		{
			Path path = file.getPath();
			long length = file.getLen();
			if (length != 0)
			{
				BlockLocation[] blkLocations;
				if (file instanceof LocatedFileStatus)
				{
					blkLocations = ((LocatedFileStatus) file)
							.getBlockLocations();
				}
				else
				{
					FileSystem fs = path.getFileSystem(job.getConfiguration());
					blkLocations = fs.getFileBlockLocations(file, 0, length);
				}
				if (isSplitable(job, path))
				{
					long blockSize = file.getBlockSize();
					long splitSize = computeSplitSize(blockSize, minSize,
							maxSize);
					long bytesRemaining = length;
					while (((double) bytesRemaining) / splitSize > SPLIT_SLOP)
					{
						int blkIndex = getBlockIndex(blkLocations, length
								- bytesRemaining);
						splits.add(makeSplit(path, length - bytesRemaining,
								splitSize,
								blkLocations[blkIndex].getHosts(),
								blkLocations[blkIndex].getCachedHosts()));
						bytesRemaining -= splitSize;
					}
					if (bytesRemaining != 0)
					{
						int blkIndex = getBlockIndex(blkLocations, length
								- bytesRemaining);
						splits.add(makeSplit(path, length - bytesRemaining,
								bytesRemaining,
								blkLocations[blkIndex].getHosts(),
								blkLocations[blkIndex].getCachedHosts()));
					}
				}
				else
				{ // not splitable
					splits.add(makeSplit(path, 0, length,
							blkLocations[0].getHosts(),
							blkLocations[0].getCachedHosts()));
				}
			}
			else
			{
				// Create empty hosts array for zero length files
				splits.add(makeSplit(path, 0, length, new String[0]));
			}
		}
		// Save the number of input files for metrics/loadgen
		job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());
		sw.stop();
		return splits;
	}
}
