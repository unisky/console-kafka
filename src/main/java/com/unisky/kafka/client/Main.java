package com.unisky.kafka.client;


import java.util.Scanner;

/**
 * Created by unisky on 2019/3/30.
 */
public class Main {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.println("******************************* welcome to this world ******************************************");
        while (true){
            String command = scanner.nextLine();
            if ("quit".equals(command)){
                System.out.println("bye bye");
                System.exit(0);
            }

            try {
                doCommand(command);
            } catch (Exception e){
                e.printStackTrace();
            }
        }
    }


    public static void doCommand(String command){
        if (command == null || command.length() == 0){
            return;
        }

        String[] slices = command.split(" ");
        switch (slices[0]){
            case "start":
                start(command);
                break;

            case "send":
                send(command);
                break;

            case "show":
                String secondParam = command.substring(slices[0].length() + 1, command.length());
                if (secondParam.equals("all")){
                    Commands.showAllReceivedMessage();
                } else {
                    Commands.showOneConsumerMessage(secondParam);
                }
                break;

            case "mute":
                secondParam = command.substring(slices[0].length() + 1, command.length());
                if (secondParam.equals("all")){
                    Commands.muteAllReceivedMessage();
                } else {
                    Commands.muteOneConsumerMessage(secondParam);
                }
                break;

            case "close":
                if (slices.length == 1){
                    System.out.println("the number of param is too few");
                    return;
                }
                Commands.deleteConsumerOrProducer(command.substring(slices[0].length() + 1, command.length()));
                break;

            case "info":
                Commands.showAllInfo();
                break;
            case "help":
                System.out.println(Config.HELP);
                break;
        }
    }

    private static void send(String command){
        String[] slices = command.split(" ");

        if (slices.length < 2){
            System.out.println("the number of param is too few");
            return;
        }

        String name = slices[1];

        if ("begin".equals(slices[2])){
            Commands.begin(name);
            return;
        }

        if ("commit".equals(slices[2])){
            Commands.commit(name);
            return;
        }

        if ("abort".equals(slices[2])){
            Commands.abort(name);
            return;
        }

        if ("topic".equals(slices[2])){
            if (slices.length < 7){
                System.out.println("the number of param is too few");
                return;
            }

            String topicName = slices[3];

            int partition = Integer.parseInt(slices[5]);

            String message = "";
            for (int i=6;i<slices.length; i++){
                message += slices[i];
                message += " ";
            }

            Commands.sendMessage(name, topicName, partition, message);
            return;
        }
    }

    private static void start(String command){
        String[] slices = command.split(" ");
        String destinationType = slices[1];
        String name= slices[2];

        if("producer".equals(destinationType)){
            if (slices.length > 3){
                Commands.startProducer(name, slices[3]);
            } else {
                Commands.startProducer(name, null);
            }
            return;
        }

        if (!"consumer".equals(destinationType)){
            return;
        }

        if (slices.length < 6){
            System.out.println("the number of param is too few");
            return;
        }

//        String topic = slices[3];
        String topicName = slices[4];
        String isolationLevel = slices[5];

        if ("rc".equals(isolationLevel)){
            Commands.startConsumer(topicName, name, true);
        }

        if ("ru".equals(isolationLevel)){
            Commands.startConsumer(topicName, name, false);
        }
    }
}
