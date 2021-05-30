public class main {
    public static void main(String[] args) {
        Manager manager = new Manager(args[0]);
        if(manager.FormPipeline())
            manager.ManageExecution();
    }
}
