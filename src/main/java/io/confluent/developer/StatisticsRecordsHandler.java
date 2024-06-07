package io.confluent.developer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.*;

public class StatisticsRecordsHandler implements ConsumerRecordsHandler<String, String>{
    ArrayList<Long> durationList;
    HashMap<String, Long> mapA;
    HashMap<String, Long> mapB;
    HashMap<String, Long> mapDuplicate;
    HashMap<String, Long> mapDuration;
    final private String topicA, topicB;
    int totalMessageCount;
    int duplicateMessageCount;
    long firstRecordInTopicATimeStamp, lastRecordInTopicBTimeStamp;
    boolean isFirstRecord;
    boolean debug;

    public StatisticsRecordsHandler(Properties consumerProps) {
        this.durationList = new ArrayList<Long>();
        this.mapA = new HashMap<String, Long>();
        this.mapB = new HashMap<String, Long>();
        this.mapDuplicate = new HashMap<String, Long>();
        this.mapDuration = new HashMap<String, Long>();
        this.topicA = consumerProps.getProperty("input.topic.name");
        this.topicB = consumerProps.getProperty("output.topic.name");
        this.totalMessageCount = 0;
        this.duplicateMessageCount = 0;
        this.isFirstRecord = true;
        this.debug = Boolean.parseBoolean(consumerProps.getProperty("debug"));
    }

    @Override
    public void process(final ConsumerRecords<String, String> consumerRecords) {
        String key;
        Long timestamp;
        HashMap<String, Long> mapAPerPoll = new HashMap<String, Long>();

        totalMessageCount = totalMessageCount + consumerRecords.count();
        for (ConsumerRecord<String, String> record : consumerRecords) {
            key = record.key();
            timestamp = Long.valueOf(record.timestamp());

            // Separate processing based on topic
            if (topicA.equals(record.topic())) {
                if (isFirstRecord) {
                    firstRecordInTopicATimeStamp = record.timestamp();
                    isFirstRecord = false;
                }

                if (mapA.containsKey(key)) {
                    duplicateMessageCount++;
                    if (this.debug) System.out.println("Duplicate consumerRecord = " + record);
                    mapDuplicate.put(key, timestamp);
                } else {
                    mapA.put(key, timestamp);
                    mapAPerPoll.put(key, timestamp);
                }; // ignore duplicates, take the first occurance
            } else if (topicB.equals(record.topic())) {
                mapB.put(key, timestamp);
                lastRecordInTopicBTimeStamp = record.timestamp();
            } else { // neither topicA nor topicB
                System.out.println("neither topicA nor topicB = " + record);
            }
        }
        processPollBasedMaps();

        printStatistics();
      }

    private void processPollBasedMaps() {
        long endToEndTime;

        Map.Entry<String, Long> entry;
        Iterator<Map.Entry<String, Long>> iterator = mapB.entrySet().iterator();
        while (iterator.hasNext()) {
            entry = iterator.next();
            if (mapA.containsKey(entry.getKey())) {
                if (!mapDuration.containsKey(entry.getKey())) {
                    endToEndTime = entry.getValue() - mapA.get(entry.getKey());
                    mapDuration.put(entry.getKey(), Long.valueOf(endToEndTime));
                    durationList.add(endToEndTime);
                }
                iterator.remove();
            }
        }
    }

    public void printStatistics() {
        Collections.sort(durationList);
        if (debug) {
            System.out.println("lastRecordInTopicBTimeStamp = " + lastRecordInTopicBTimeStamp);
            System.out.println("firstRecordInTopicATimeStamp = " + firstRecordInTopicATimeStamp);
            System.out.println("MapA Size= " + mapA.size());
            System.out.println("MapB size = " + mapB.size());
            System.out.println("MapDuplicate size = " + mapDuplicate.size());
            System.out.println("MapDuration size = " + mapDuration.size());
            System.out.println("durationList size = " + durationList.size());
        }

        if (!durationList.isEmpty()) {
            System.out.println(
                      " Total message count=" + (mapA.size() + duplicateMessageCount)
                    + " Duplicate message count=" + duplicateMessageCount
                    + " Throughput=" + (mapA.size() + duplicateMessageCount) * 1000 / ((lastRecordInTopicBTimeStamp - firstRecordInTopicATimeStamp)) + "/s " // requirement > 65k/s
                    + " Latency Mean=" + findMean() + "ms "
                    + " Min=" + durationList.get(0) + "ms "
                    + " Median=" + findMedian()  + "ms "                // requirement < 5 s
                    + " 95percentile=" + find95percentile() + "ms "     // requirement < 10s
                    + " Max=" + durationList.get(durationList.size() - 1) + "ms "
                ) ;
        }
    }

    private long findMean() {
        long sum = 0;
        //if (durationList.isEmpty()) return 0;
        for (long value : durationList) {
            sum = sum + value;
        }
        return (sum/durationList.size());
    }

    private long findMedian() {
        int medianPosition = 0;
        long value = 0;

        if (durationList.size()%2 == 0) { //even
            medianPosition = durationList.size()/2;
            value = (findElementAtPosition(medianPosition) + findElementAtPosition(medianPosition-1))/2;
        } else { // odd
            medianPosition = (durationList.size()-1)/2;
            value = findElementAtPosition(medianPosition);
        }
        return value;
    }

    private long findElementAtPosition(int position) {
        Iterator<Long> it = durationList.iterator();  //in ascending order
        int i = 0;
        long target = 0;
        while(it.hasNext() && i <= position) {
            target = it.next();
            i++;
        }
        return target;
    }

    private long find95percentile() {
        int index = (durationList.size())*95/100;
        return findElementAtPosition(index);
    }
}
