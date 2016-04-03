package com.zzm.datacount.domain;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DataBean implements Writable{
	
	private String telNo;//手机号码
	
	private long upPayLoad;//上行流量
	
	private long downPayLoad;//下行流量
	
	private long totalPayLoad;//总流量
	
	/**
	 * 反序列化
	 * 读取要注意顺序，类型
	 */
	public void readFields(DataInput in) throws IOException {
		this.telNo = in.readUTF();
		this.upPayLoad = in.readLong();
		this.downPayLoad = in.readLong();
		this.totalPayLoad = in.readLong();
	}
	
	/**
	 * 序列化
	 */
	public void write(DataOutput out) throws IOException {
		out.writeUTF(telNo);
		out.writeLong(upPayLoad);
		out.writeLong(downPayLoad);
		out.writeLong(totalPayLoad);
	}

	public DataBean() {
		super();
	}

	public DataBean(String telNo, long upPayLoad, long downPayLoad) {
		super();
		this.telNo = telNo;
		this.upPayLoad = upPayLoad;
		this.downPayLoad = downPayLoad;
		this.totalPayLoad = upPayLoad + downPayLoad;
	}

	public String getTelNo() {
		return telNo;
	}

	public void setTelNo(String telNo) {
		this.telNo = telNo;
	}

	public long getUpPayLoad() {
		return upPayLoad;
	}

	public void setUpPayLoad(long upPayLoad) {
		this.upPayLoad = upPayLoad;
	}

	public long getDownPayLoad() {
		return downPayLoad;
	}

	public void setDownPayLoad(long downPayLoad) {
		this.downPayLoad = downPayLoad;
	}
 
	public long getTotalPayLoad() {
		return totalPayLoad;
	}

	public void setTotalPayLoad(long totalPayLoad) {
		this.totalPayLoad = totalPayLoad;
	}

	@Override
	public String toString() {
		return this.telNo + "\t" + this.upPayLoad + "\t" + this.downPayLoad + "\t" + this.totalPayLoad + "\t";
	}

}
