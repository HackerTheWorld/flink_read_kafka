package nettyNIO.hander.flink;

public class KeyVo {
    
    public String jsonId;
    public String mouldNoSys;

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

    /**
     * 将自定义类作为keby需要重定义hashcode和equals
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((mouldNoSys == null) ? 0 : mouldNoSys.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        KeyVo other = null;
        if(obj instanceof KeyVo){
            other = (KeyVo) obj;
        }else{
            return false;
        }
        return other.getMouldNoSys().equals(mouldNoSys);
        
    }

    
    

}
