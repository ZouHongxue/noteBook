package com.notebook.test;

class ListNode {
	     int val;
	     ListNode next;
	     ListNode(int x) { val = x; }
 }

public class Solution_AddTwoNumber {
	
	public static ListNode addTwoNumbers(ListNode l1, ListNode l2) {
//		ListNode root = new ListNode(0);
//		ListNode tmp = root;
//		root.val = l1.val;
//		int add = 0;
//		while (l1!=null||l2!=null) {
//			int a = (l1 == null) ? 0 :l1.val;
//			int b = (l2 == null) ? 0 :l2.val;
//			int sum = a+b+add;
//			add=sum/10;
//			tmp.next = new ListNode(sum%10);
//			tmp = tmp.next;
//			if (l1!=null) 
//				l1 = l1.next;
//			if (l2!=null)
//				l2 = l2.next;
//		}
//		if (add>0) {
//			tmp.next = new ListNode(add);
//		}
//		return root.next;
		int sum=0,append=0,left=0;  
        ListNode root=new ListNode(0);  
        ListNode result=root;  
        while(l1!=null || l2!=null){                      
            int v1=l1!=null?l1.val:0;  
            int v2=l2!=null?l2.val:0;  
            sum=v1+v2+append;  
            append=sum/10;  
            left  =sum%10;  
            ListNode cursor=new ListNode(left);  
            result.next=cursor;  
            result=cursor;  
            if(l1!=null)  
                l1=l1.next;  
            if(l2!=null)  
                l2=l2.next;  

        }  
        if(append>0){  
            ListNode cursor=new ListNode(append);  
            result.next=cursor;  
        }  

        return root.next;  

    }
	
	public static void main(String[] args) {
		ListNode l1r,l2r ;
		ListNode l1 = new ListNode(2);
		l1r = l1;
		l1.next = new ListNode(4);
		l1 = l1.next;
		l1.next = new ListNode(3);
		ListNode l2 = new ListNode(2);
		l2r = l2;
		l2.next = new ListNode(8);
		l2 = l2.next;
		l2.next = new ListNode(2);
		ListNode l3 = addTwoNumbers(l1r, l2r);
		while (l3!=null) {
			System.out.println(l3.val);
			l3 = l3.next;
		}
	}
}
