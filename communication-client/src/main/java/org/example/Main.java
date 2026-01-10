package org.example;

import java.util.Scanner;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) {
        //TIP Press <shortcut actionId="ShowIntentionActions"/> with your caret at the highlighted text
        // to see how IntelliJ IDEA suggests fixing it.
        System.out.printf("Hello and welcome!");
        Scanner userInput = new Scanner(System.in);
        System.out.println("1. Enter Account ID,amount to set the balance :\n2. Enter Account ID to Check Balance\n");

        String inputList = userInput.nextLine().trim();
        String[] inputs =null;
        inputs = inputList.split("\\s*,\\s*");
        System.out.println("Inputs : "+inputs.length);
        for(String s : inputs){
            System.out.println("Inputs elements : "+s);
        }


    }
}