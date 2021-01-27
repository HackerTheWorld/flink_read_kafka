package flinksummary.vo;

public class KafkaMessageVo{

    private String jsonId;
    private String mouldNoSys;
    private int num;
    private String testTime;

    public String getJsonId() {
        return jsonId;
    }

    public void setJsonId(String jsonId) {
        this.jsonId = jsonId;
    }

    public String getMouldNoSys() {
        return mouldNoSys;
    }

    public void setMouldNoSys(String mouldNoSys) {
        this.mouldNoSys = mouldNoSys;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public String getTestTime() {
        return testTime;
    }

    public void setTestTime(String testTime) {
        this.testTime = testTime;
    }

    

}