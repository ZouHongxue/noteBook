package com.notebook.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.notebook.service.NoteService;

@Controller
@RequestMapping("/note")
public class NoteController {
	
	private String user = "admin";
	
	@Autowired
	NoteService noteService;
	
	@RequestMapping("/getNotes")
	@ResponseBody
	public String getNotes(String rowKey){
		System.out.println(rowKey);
		String notes = null;
		notes = noteService.getNotes(rowKey);
		return notes;
	}
	
	@RequestMapping(value="/add",produces="text/html;charset=UTF-8")
	@ResponseBody
	public String add(String info,String name){
		System.out.println(info.trim().split(":")[0]+"\t"+name);
		if (noteService.addNote(info.trim().split(":")[0], name)) {
			return "添加成功";
		}else {
			return "添加失败";
		}
	}
	
	@RequestMapping(value="/saveNote",produces="text/html;charset=UTF-8")
	@ResponseBody
	public String saveNote(String note,String text){
		System.out.println("需要保存的内容"+text);
		if(noteService.saveNote(user, note, text)){
			return "保存成功";
		}else {
			return "保存失败";
		}
	}
	
	@RequestMapping(value="/getNoteText",produces="text/html;charset=UTF-8")
	@ResponseBody
	public String getNoteText(String note){
		String text = noteService.getNoteText(user, note);
		System.out.println("获取到的内容："+text);
		return text;
	}
	
	@RequestMapping("/test")
	public void test(){
	}
}
