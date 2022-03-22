import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.bson.Document;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

public class Main {

    private static final String PATH = "/Users/savelijgevel/desktop/mongo.csv";

    public static void main(String[] args) {

        MongoClient mongoClient = new MongoClient("127.0.0.1" , 27017);
        MongoDatabase mongoDatabase = mongoClient.getDatabase("local");

        MongoCollection<Document> students = mongoDatabase.getCollection("students");

        parseDocumentAndInsertStudentsInfoToDB(students);

        System.out.println("Общее количество студентов в базе:");
        System.out.println(students.countDocuments());

        System.out.println("Количество студентов старше сорока лет:");
        System.out.println(students.countDocuments(BsonDocument.parse("{age : {$gt : \"40\"}}")));

        System.out.println("Имя самого молодого студента:");
        System.out.println(students.find().sort(BsonDocument.parse("{age : 1}")).first().get("name"));

        System.out.println("Спиок курсов самого старого студента:");
        System.out.println(students.find().sort(BsonDocument.parse("{age : -1}")).first().get("courses"));

    }

    public static void parseDocumentAndInsertStudentsInfoToDB(MongoCollection<Document> students) {

        students.drop();

        try {
            List<String> lines = Files.readAllLines(Path.of(PATH));

            lines.forEach(line -> {
                String[] studentInfo = line.split(",", 3);

                String name = studentInfo[0];
                String age = studentInfo[1];
                String[] coursesArray = studentInfo[2].substring(1, studentInfo[2].length() - 1).split(",");
                List<String> courses = Arrays.stream(coursesArray).toList();

                students.insertOne(new Document().append("name", name)
                        .append("age", age)
                        .append("courses", courses));

            });
        } catch (IOException e) {
            e.printStackTrace();
        }

    }




}
