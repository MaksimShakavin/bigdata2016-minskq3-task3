package com.epam.bigdata2016.minskq3.task3.visitcount.model;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserLog implements WritableComparable<UserLog> {

    private long visitsCount;
    private long spendsCount;

    public UserLog() {
    }

    public UserLog(long visitsCount, long spendsCount) {
        this.visitsCount = visitsCount;
        this.spendsCount = spendsCount;
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(visitsCount);
        out.writeLong(spendsCount);
    }

    public void readFields(DataInput in) throws IOException {
        visitsCount = in.readLong();
        spendsCount = in.readLong();
    }

    public int compareTo(UserLog w) {
        if (Long.compare(visitsCount, w.getVisitsCount()) == 0) {
            return Long.compare(spendsCount, w.getSpendsCount());
        } else {
            return Long.compare(visitsCount, w.getVisitsCount());
        }
    }

    public long getVisitsCount() {
        return visitsCount;
    }

    public void setVisitsCount(long visitsCount) {
        this.visitsCount = visitsCount;
    }

    public long getSpendsCount() {
        return spendsCount;
    }

    public void setSpendsCount(long spendsCount) {
        this.spendsCount = spendsCount;
    }

    public void addVisits(long visits) {
        this.visitsCount += visits;
    }

    public void addSpends(long spends) {
        this.spendsCount += spends;
    }

    @Override
    public String toString() {
        return "Visits count : " + visitsCount +
                ", Bidding price sum : " + spendsCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        UserLog userLog = (UserLog) o;

        if (visitsCount != userLog.visitsCount) return false;
        return spendsCount == userLog.spendsCount;

    }

    @Override
    public int hashCode() {
        int result = (int) (visitsCount ^ (visitsCount >>> 32));
        result = 31 * result + (int) (spendsCount ^ (spendsCount >>> 32));
        return result;
    }
}