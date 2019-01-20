package com.elasticseach;

import com.fasterxml.jackson.annotation.JsonIdentityReference;

@JsonIdentityReference
public class Attachment {
	
	private String id;
	private String content;
	private String date;
	private String content_type;
	private String language;
	private String content_length;
	private String author;
	private String title;

	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getContent() {
		return content;
	}
	public String getAuthor() {
		return author;
	}
	public void setAuthor(String author) {
		this.author = author;
	}
	public void setContent(String content) {
		this.content = content;
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public String getContent_type() {
		return content_type;
	}
	public void setContent_type(String content_type) {
		this.content_type = content_type;
	}
	public String getLanguage() {
		return language;
	}
	public void setLanguage(String language) {
		this.language = language;
	}
	public String getContent_length() {
		return content_length;
	}
	public void setContent_length(String content_length) {
		this.content_length = content_length;
	}
	@Override
	public String toString() {
		return "Attachment [id=" + id + ", path=" + path+ ", date=" + date + ", content_type=" + content_type
				+ ", language=" + language + ", content_length=" + content_length + ", author=" + author + ", title="
				+ title + "]";
	}
}
