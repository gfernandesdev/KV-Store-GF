import java.io.Serializable;

public class Mensagem implements Serializable {
    private static final long serialVersionUID = 1L;
    private String comando;
    private String key;
    private String value;
    private String response;

    public Mensagem(String comando, String key, String value) {
        this.comando = comando;
        this.key = key;
        this.value = value;
    }

    public Mensagem(String response) {
        this.response = response;
    }

    public String getComando() {
        return comando;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public String getResponse() {
        return response;
    }
}


