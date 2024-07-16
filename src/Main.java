import java.sql.*;

public class Main {
    private static final String PROTOCOL = "jdbc:postgresql://";        // URL-prefix
    private static final String DRIVER = "org.postgresql.Driver";       // Driver name
    private static final String URL_LOCALE_NAME = "localhost/";         // ваш компьютер + порт по умолчанию

    private static final String DATABASE_NAME = "computers";          // имя базы

    public static final String DATABASE_URL = PROTOCOL + URL_LOCALE_NAME + DATABASE_NAME;
    public static final String USER_NAME = "postgres";                  // имя пользователя
    public static final String DATABASE_PASS = "postgres";              // пароль базы данных

    public static void main(String[] args) {
        checkDriver();
        checkDB();
        System.out.println("Подключение к базе данных | " + DATABASE_URL + "\n");
        try (Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS)) {
            // Вывод всех таблиц
            System.out.println("Видеокарты: " );
            getGpu(connection); System.out.println();
            System.out.println("Процессоры: ");
            getCpu(connection); System.out.println();
            System.out.println("Сборщики компьютеров: ");
            getPcBuilders(connection); System.out.println();
            System.out.println("Конфигурации ПК: ");
            getPcModels(connection); System.out.println();
            // Все запросы
            System.out.println("Все конфигурации дороже 100000р с сортировкой по убыванию цены: ");
            getFirstQuery(connection); System.out.println();
            System.out.println("Все конфигурации с объемом оперативной памяти не менее 32 Гб : ");
            getSecondQuery(connection); System.out.println();
            System.out.println("Вывод всех процессоров с количеством потоков больше 8 : ");
            getThirdQuery(connection);
            System.out.println("Вывод всех видеокарт с объемом памяти больше 6 Гб :  ");
            getFourthQuery(connection);
            System.out.println("Вывод всех видеокарт производителя Gigabyte:  ");
            getFifthQuery(connection);
            System.out.println("Добавление видеокарты: ");
            addGPU(connection,15,"GT 210", 1, "Palit");
            System.out.println("Обновление видеокарты: ");
            updateGPU(connection, 15,"RTX 2080ti SUPER", 11, "MSI");
            System.out.println("Удаление видеокарты: ");
            deleteGPU(connection, "RTX 2080ti SUPER");
            System.out.println("Замена cpu_id на модель процессора");
            cpuJoinToConfigs(connection);
            System.out.println("Замена gpu_id на модель видеокарты");
            gpuJoinToConfigs(connection);



        } catch (SQLException e) {
            if (e.getSQLState().startsWith("23")){
                System.out.println("Произошло дублирование данных");
            } else throw new RuntimeException(e);
        }

    }
    static void getGpu (Connection connection) throws SQLException {
        int param0 = -1; String param1 = null; int param2 = -1; String param3 = null;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM public.gpu;");

        while (rs.next()) {
            param0 = rs.getInt(1);
            param1 = rs.getString(2);
            param2 = rs.getInt(3);
            param3 = rs.getString(4);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | ");
        }
    }

    static void getCpu (Connection connection) throws SQLException {
        int param0 = -1; String param1 = null; int param2 = -1; int param3 = -1; double param4 = -1.0; int param5 = -1;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM public.cpu;");

        while (rs.next()) {
            param0 = rs.getInt(1);
            param1 = rs.getString(2);
            param2 = rs.getInt(3);
            param3 = rs.getInt(4);
            param4 = rs.getDouble(5);
            param5 = rs.getInt(6);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4 + " | " + param5 + " | " );
        }
    }

    static void getPcBuilders (Connection connection) throws SQLException {
        int param0 = -1; String param1 = null; String param2 = null;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM public.pc_builders;");

        while (rs.next()) {
            param0 = rs.getInt(1);
            param1 = rs.getString(2);
            param2 = rs.getString(3);

            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " );
        }
    }
    static void getPcModels (Connection connection) throws SQLException {
        int param0 = -1; int param1 = -1; int param2 = -1; int param3 = -1; int param4 = -1; int param5 = -1; int param6 = -1;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM public.pc_models;");

        while (rs.next()) {
            param0 = rs.getInt(1);
            param1 = rs.getInt(2);
            param2 = rs.getInt(3);
            param3 = rs.getInt(4);
            param4 = rs.getInt(5);
            param5 = rs.getInt(6);
            param6 = rs.getInt(7);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4 + " | " + param5 + " | " + param6);
        }
    }

    static void getFirstQuery (Connection connection) throws SQLException {
        int param0 = -1; int param1 = -1; int param2 = -1; int param3 = -1; int param4 = -1; int param5 = -1; int param6 = -1;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM public.pc_models\n" +
                "WHERE price > 100000\n" +
                "ORDER BY price DESC;");

        while (rs.next()) {
            param0 = rs.getInt(1);
            param1 = rs.getInt(2);
            param2 = rs.getInt(3);
            param3 = rs.getInt(4);
            param4 = rs.getInt(5);
            param5 = rs.getInt(6);
            param6 = rs.getInt(7);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4 + " | " + param5 + " | " + param6);
        }
    }

    static void getSecondQuery (Connection connection) throws SQLException {
        int param0 = -1; int param1 = -1; int param2 = -1; int param3 = -1; int param4 = -1; int param5 = -1; int param6 = -1;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM public.pc_models\n" +
                "WHERE ram >= 32\n" +
                "ORDER BY id;");

        while (rs.next()) {
            param0 = rs.getInt(1);
            param1 = rs.getInt(2);
            param2 = rs.getInt(3);
            param3 = rs.getInt(4);
            param4 = rs.getInt(5);
            param5 = rs.getInt(6);
            param6 = rs.getInt(7);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4 + " | " + param5 + " | " + param6);
        }
    }

    static void getThirdQuery (Connection connection) throws SQLException {
        int param0 = -1; String param1 = null; int param2 = -1; int param3 = -1; double param4 = -1.0; int param5 = -1;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM public.cpu\n" +
                "WHERE threads > 8\n" +
                "ORDER BY id ASC;");

        while (rs.next()) {
            param0 = rs.getInt(1);
            param1 = rs.getString(2);
            param2 = rs.getInt(3);
            param3 = rs.getInt(4);
            param4 = rs.getDouble(5);
            param5 = rs.getInt(6);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4 + " | " + param5 + " | " );
        }
    }
    static void getFourthQuery (Connection connection) throws SQLException {
        int param0 = -1; String param1 = null; int param2 = -1; String param3 = null;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM public.gpu\n" +
                "WHERE video_memory > 6\n" +
                "ORDER BY id ASC;");

        while (rs.next()) {
            param0 = rs.getInt(1);
            param1 = rs.getString(2);
            param2 = rs.getInt(3);
            param3 = rs.getString(4);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | ");
        }
    }
    static void getFifthQuery (Connection connection) throws SQLException {
        int param0 = -1; String param1 = null; int param2 = -1; String param3 = null;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM public.gpu\n" +
                "WHERE vendor = 'GIGABYTE'\n" +
                "ORDER BY id ASC;");

        while (rs.next()) {
            param0 = rs.getInt(1);
            param1 = rs.getString(2);
            param2 = rs.getInt(3);
            param3 = rs.getString(4);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | ");
        }

    }

    private static void addGPU(Connection connection, int id, String model, int memory, String vendor) throws SQLException{
        if (model == null || model.isBlank() || memory < 0 || vendor == null || vendor.isBlank()) return;

        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO public.gpu VALUES (?, ?, ?, ?)"
        );
        statement.setInt(1, id);
        statement.setString(2, model);
        statement.setInt(3, memory);
        statement.setString(4, vendor);

        int count = statement.executeUpdate();

        ResultSet rs = statement.getGeneratedKeys();

        if (rs.next()) {
            System.out.println("Идентификатор видеокарты " + rs.getInt(1));
        }

        System.out.println("INSERTed " + count + " gpu");
        getGpu(connection);
    }

    private static void updateGPU(Connection connection,int id, String model, int memory, String vendor) throws SQLException{
        if (model == null || model.isBlank() || memory < 0 || vendor == null || vendor.isBlank()) return;

        PreparedStatement statement = connection.prepareStatement(
                "UPDATE public.gpu SET model = ?, video_memory = ?, vendor = ? WHERE id = ?;"
        );
        statement.setString(1, model);
        statement.setInt(2, memory);
        statement.setString(3, vendor);
        statement.setInt(4,id);

        int count = statement.executeUpdate();

        ResultSet rs = statement.getGeneratedKeys();


        System.out.println("UPDATEd " + count + " gpu");
        getGpu(connection);
    }
    private static void deleteGPU(Connection connection, String model) throws SQLException{
        if (model == null || model.isBlank()) return;
        PreparedStatement statement = connection.prepareStatement("DELETE FROM public.gpu WHERE model = ?");
        statement.setString(1, model);

        int count = statement.executeUpdate();
        System.out.println("DELETEd " + count + " gpu");
        getGpu(connection);
    }

    private static void cpuJoinToConfigs(Connection connection) throws SQLException{
        int param0 = -1; String param1 = null; int param2 = -1; int param3 = -1; int param4 = -1; int param5 = -1;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT pc_models.id, cpu.model AS cpu, ram, gpu_id, ssd, price\n" +
                "FROM pc_models\n" +
                "LEFT JOIN cpu\n" +
                "ON pc_models.cpu_id = cpu.id;");

        while (rs.next()) {
            param0 = rs.getInt(1);
            param1 = rs.getString(2);
            param2 = rs.getInt(3);
            param3 = rs.getInt(4);
            param4 = rs.getInt(5);
            param5 = rs.getInt(6);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4 + " | " + param5);
        }

    }
    private static void gpuJoinToConfigs(Connection connection) throws SQLException{
        int param0 = -1; int param1 = -1; int param2 = -1; String param3 = null; int param4 = -1; int param5 = -1;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT pc_models.id, cpu_id, ram, gpu.model AS gpu, ssd, price\n" +
                "FROM pc_models\n" +
                "LEFT JOIN gpu\n" +
                "ON pc_models.gpu_id = gpu.id;");

        while (rs.next()) {
            param0 = rs.getInt(1);
            param1 = rs.getInt(2);
            param2 = rs.getInt(3);
            param3 = rs.getString(4);
            param4 = rs.getInt(5);
            param5 = rs.getInt(6);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4 + " | " + param5);
        }

    }

    public static void checkDriver () {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Нет JDBC-драйвера! Подключите JDBC-драйвер к проекту согласно инструкции.");
            throw new RuntimeException(e);
        }
    }

    public static void checkDB () {
        try {
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS);
        } catch (SQLException e) {
            System.out.println("Нет базы данных! Проверьте имя базы, путь к базе или разверните локально резервную копию согласно инструкции");
            throw new RuntimeException(e);
        }
    }
}