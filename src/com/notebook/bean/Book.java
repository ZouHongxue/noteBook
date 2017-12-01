package com.notebook.bean;

public class Book {
	
	private String user;
	private String rowKey;
	private String book;
	public Book(String user, String rowKey, String book) {
		super();
		this.user = user;
		this.rowKey = rowKey;
		this.book = book;
	}
	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
	public String getRowKey() {
		return rowKey;
	}
	public void setRowKey(String rowKey) {
		this.rowKey = rowKey;
	}
	public String getBook() {
		return book;
	}
	public void setBook(String book) {
		this.book = book;
	}
	
	
}
