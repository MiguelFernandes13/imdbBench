import com.beust.jcommander.Parameter;

public class Options {
    @Parameter(names = {"-h","-?","--help"}, help = true, description = "display usage information")
    public boolean help;

    @Parameter(names = {"-d","--database"}, description = "JDBC database url")
    public String database = "jdbc:postgresql://localhost/bench";

    @Parameter(names = {"-U","--user"}, description = "database user name")
    public String user = null;

    @Parameter(names = {"-P","--password"}, description = "database password")
    public String passwd = null;

    @Parameter(names = {"-W","--warmup"}, description = "warmup time")
    public int warmup = 5;

    @Parameter(names = {"-R","--runtime"}, description = "run time")
    public int runtime = 10;

    @Parameter(names = {"-c","--clients"}, description = "number of client threads")
    public int clients = 1;
}
