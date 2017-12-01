<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<%
String path = request.getContextPath();
String basePath = request.getScheme()+"://"+request.getServerName()+":"+request.getServerPort()+path+"/";
%>
<!DOCTYPE html>
<html>
	<head>
		<meta charset="UTF-8">
		<title></title>
		<style>
			*{
				padding: 0;
				margin: 0;
			}
			ul{
				list-style:none;
			}
			.lf{
				float: left;
				border: 2px solid;
				height: 600px;
				z-index: 2;
			}
			.w2{
				width: 20%;
			}
			.w6{
				width: 58%;
			}
			.add{
				width: 200px;
				height: 80px;
				border: 1px solid;
				position: fixed;
				top: 35%;
				left: 45%;
				z-index: 1;
				display: none;
			}
			#u1{
				margin-top:50px;
			}
			#d2{
				margin-top:50px;
			}
			#content{
				width:60%;
				height:50%;
				border:1px solid;
				display:none;
				margin:30px;
			}
			#content div{
				height:90%;
				border:1px solid red;
			}
		</style>
	</head>
	<body>
		<div class="lf w2">
			<div>
				<input type="button" value="添加笔记本" onclick="showAddBook()"/>
			</div>
			<div>
				<ul id="u1">
					<c:if test="${books!=null }">
						<c:forEach var="book" items="${books }">
							<li name="book">
							<!--  <%=path%>/note/getNotesByKey?key=${book.rowKey}&name=${book.book }-->
							<a href="javascript:void(0);" onclick="showNotes('${book.rowKey }')">${book.rowKey}:${book.book }</a>
							</li>
						</c:forEach>
					</c:if>
				</ul>
			</div>
		</div>
		<div class="lf w2">
			<div>
				<input type="button" value="添加笔记" onclick="showAddNote()"/>
			</div>
			<div id="d2">
				笔记列表：
				<ul id="u2">
					
				</ul>
			</div>
		</div>
		<div class="lf w6">
			<div id="content">
				<div id="text" contenteditable="true">
					
				</div>
				<input type="button" value="提交" onclick="saveText()">
			</div>
		</div>
		<div class="add" id="addBook">
			请输入笔记本名称:<input id="book" /> <input type="button" value="确定" onclick="addNoteBook()"/>
		</div>
		<div class="add" id="addNote">
			请选择保存笔记本：<select id="s1"></select>
			请输入笔记名称:<input id="namen" /> <input type="button" value="确定" onclick="addNote()"/>
		</div>
	</body>
	<script src="<%=path %>/js/jquery.min.js"></script>
	<script>
		
		function showAddBook(){
			$("#addBook").css("display","block");
		}
		
		function addNoteBook(){
			$("#add").css("display","none");
			$.ajax({
				url:"<%=request.getContextPath()%>/book/add.action",
				type:"POST",
				data:{"name":$("#book").val()},
				success:function(msg){
					alert(msg);
					if(msg=="添加成功"){
						location.reload(true);
					}
				}
			})
		}
		
		function showAddNote(){
			$("#addNote").css("display","block");
			$("li[name='book']").each(function(){
				$("#s1").append("<option>"+$(this).text()+"</option>");
			})
		}
		
		function addNote(){
			$("#add").css("display","none");
			$.ajax({
				url:"<%=request.getContextPath()%>/note/add.action",
				type:"POST",
				data:{
					"info":$("#s1 option:selected").text(),
					"name":$("#namen").val()
				},
				success:function(msg){
					alert(msg);
				}
			})
		}
		
		function showNotes(rowKey){
			$("#u2").empty();
			$.ajax({
				url:"<%=request.getContextPath()%>/note/getNotes.action",
				type:"POST",
				data:{
					"rowKey":rowKey
				},
				success:function(msg){
					if(msg==" "){
						alert("该笔记本下没有笔记");
					}else{
						var notes = new Array();
						notes = msg.split(",");
						 $.each(notes, function (index, note) {
						 	 $("#u2").append("<li><a onclick="+"\""+"showEdit('"+note+"')"+"\""+" href='javascript:void(0)'>"+note+"</a></li>");
						 });
					}
				}
			})
		}
		
		function showEdit(note){
			$("#text").empty();
			$("#content").css("display","block");
			$("#text").attr("name",note);
			$.ajax({
				url:"<%=request.getContextPath()%>/note/getNoteText.action",
				type:"POST",
				data:{
					"note":$("#text").attr("name")
				},
				success:function(msg){
					$("#text").html(msg);
				}
			})
		}
		
		function saveText(){
			$.ajax({
				url:"<%=request.getContextPath()%>/note/saveNote.action",
				type:"POST",
				data:{
					"note":$("#text").attr("name"),
					"text":$("#text").text()
				},
				success:function(msg){
					alert(msg);
				}
			})
		}
	</script>
</html>
