package com.udacity.webcrawler;

import com.udacity.webcrawler.json.CrawlResult;
import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A concrete implementation of {@link WebCrawler} that runs multiple threads on a
 * {@link ForkJoinPool} to fetch and process multiple web pages in parallel.
 */
final class ParallelWebCrawler implements WebCrawler {
  private final Clock clock;
  private final Duration timeout;
  private final int popularWordCount;
  private final ForkJoinPool pool;
  private final PageParserFactory pageParserFactory;
  private final int maxDepth;
  private final List<Pattern> ignoredUrls;


  @Inject
  ParallelWebCrawler(
      Clock clock,
      @Timeout Duration timeout,
      @PopularWordCount int popularWordCount,
      @TargetParallelism int threadCount,
      PageParserFactory pageParserFactory,
      @MaxDepth int maxDepth,
      @IgnoredUrls List<Pattern> ignoredUrls) {
    this.clock = clock;
    this.timeout = timeout;
    this.popularWordCount = popularWordCount;
    this.pool = new ForkJoinPool(Math.min(threadCount, getMaxParallelism()));
    this.pageParserFactory = pageParserFactory;
    this.maxDepth = maxDepth;
    this.ignoredUrls = ignoredUrls;
  }

  @Override
  public CrawlResult crawl(List<String> startingUrls) {
    Instant deadline = clock.instant().plus(timeout);
    Map<String, AtomicInteger> counts = new ConcurrentHashMap<>();
    Set<String> visitedUrls = new ConcurrentSkipListSet<>();

    for (String url : startingUrls) {
      ParallelTask parallelTask = new ParallelTask(url, deadline, maxDepth, counts, visitedUrls);
      this.pool.invoke(parallelTask);
    }

    Map<String, Integer> integerCountsMap = new HashMap<>();
    for (Map.Entry<String, AtomicInteger> entry : counts.entrySet()) {
      int countValue = entry.getValue().get();
      integerCountsMap.put(entry.getKey(), countValue);
    }

    if (counts.isEmpty()) {
      return new CrawlResult.Builder()
              .setWordCounts(integerCountsMap)
              .setUrlsVisited(visitedUrls.size())
              .build();
    }

    return new CrawlResult.Builder()
            .setWordCounts(WordCounts.sort(integerCountsMap, popularWordCount))
            .setUrlsVisited(visitedUrls.size())
            .build();
  }


  private class ParallelTask extends RecursiveTask<Void> {
    private final String url;
    private final Instant deadline;
    private final int depth;
    private final Map<String, AtomicInteger> counts;
    private final Set<String> visitedUrls;

    private ParallelTask(String url, Instant deadline, int maxDepth, Map<String, AtomicInteger> counts, Set<String> visitedUrls) {
      this.url = url;
      this.deadline = deadline;
      this.depth = maxDepth;
      this.counts = counts;
      this.visitedUrls = visitedUrls;
    }

    @Override
    protected Void compute() {
      if (this.depth <= 0 || ParallelWebCrawler.this.clock.instant().isAfter(this.deadline)) {
        return null;
      }

      if (ParallelWebCrawler.this.ignoredUrls.stream().anyMatch(pattern -> pattern.matcher(this.url).matches())) {
        return null;
      }

      if(!visitedUrls.add(url)) {
        return null;
      }

      PageParser.Result result = pageParserFactory.get(url).parse();
      Map<String, Integer> resultWordCounts = result.getWordCounts();
      for (Map.Entry<String, Integer> entry : resultWordCounts.entrySet()) {
        String key = entry.getKey();
        Integer countResult = entry.getValue();
        this.counts.computeIfAbsent(key, k -> new AtomicInteger(0)).addAndGet(countResult);
      }


      List<String> resultLinks = result.getLinks();
      List<ParallelTask> subtasks = new ArrayList<>();
      for (String link : resultLinks) {
        ParallelTask parallelTask = new ParallelTask(link, this.deadline, this.depth - 1, this.counts, this.visitedUrls);
        subtasks.add(parallelTask);
      }

      RecursiveTask.invokeAll(subtasks);

      for (ParallelTask subTask : subtasks) {
        try {
          subTask.get();
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
      }

      return null;
    }
  }

  @Override
  public int getMaxParallelism() {
    return Runtime.getRuntime().availableProcessors();
  }
}
