import java.util.Scanner;

public class Driver {
    public static void main(String[] args) throws Exception {
        Scanner scanner = new Scanner(System.in);

        System.out.println("Enter a number between 1 and 2:");
        int choice = scanner.nextInt();

        switch (choice) {
            case 1:
                PreprocessJob.run();
                break;
            case 2:
                AnimeVarianceJob.run();
                break;
            default:
                System.out.println("Invalid choice.");
        }
    }
}