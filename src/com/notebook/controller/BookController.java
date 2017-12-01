package com.notebook.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.notebook.bean.Book;
import com.notebook.hbase.HBase;
import com.notebook.hbase.HDFSUtils;
import com.notebook.service.NoteBookService;

@Controller
@RequestMapping("/book")
public class BookController {

	private String user = "admin";
	
	@Autowired
	private NoteBookService noteBookService ;
	
	@RequestMapping(value="/add",produces="text/html;charset=UTF-8")
	@ResponseBody
	public String add(String name){
		boolean f = noteBookService.addBook(user, name);
		if (f) {
			return "添加成功";
		}else {
			return "添加失败";
		}
	}
	
	@RequestMapping("/getAll")
	public String getAll(Model model){
		List<Book> books = noteBookService.getAll(user);
		model.addAttribute("books", books);
		return "test";
	}
	
	@RequestMapping("/test")
	public String test(){
		noteBookService.test();
		return "";
	}
}
