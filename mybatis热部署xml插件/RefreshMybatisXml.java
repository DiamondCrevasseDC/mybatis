import org.apache.ibatis.builder.xml.XMLMapperBuilder;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

@RestController
public class RefreshMybatisXml {

    private static final Logger logger = LoggerFactory.getLogger(RefreshMybatisXml.class);

    @Autowired
    private SqlSessionFactory sqlSessionFactory;

    private Resource[] mapperLocations;

    private final String MAPPED_STATEMENTS = "mappedStatements";
    private final String CACHES = "caches";
    private final String RESULT_MAPS = "resultMaps";
    private final String PARAMETER_MAPS = "parameterMaps";
    private final String KEY_GENERATORS = "keyGenerators";
    private final String SQL_FRAGMENTS = "sqlFragments";
    private final String LOADED_RESOURCES = "loadedResources";

    /**
     * 扫描路径
     */
    private static final String scanPath = "classpath*:/mybatis/*.xml";

    /**
     * 记录文件是否变化
     */
    private static Map<String, Long> fileMapping = new HashMap<>(16);

    /**
     * 初始化fileMapping
     */
    static {
        try {
            Resource[] tempMapperLocations = new PathMatchingResourcePatternResolver().getResources(scanPath);
            for(Resource resource : tempMapperLocations){
                String resourceName = resource.getFilename();
                long lastFrame = resource.contentLength() + resource.lastModified();
                fileMapping.put(resourceName, Long.valueOf(lastFrame));
            }
        } catch (Exception e) {
            logger.error("mybatis热部署初始化异常！", e);
        }
    }

    @RequestMapping(value = "refreshXml", method = RequestMethod.GET)
    public Map<String, Object> refreshMapperXml(){
        System.out.println("=======开始检查mybatis映射文件是否更新========");
        Map<String, Object> results = new HashMap<>(8);
        try {
            Configuration configuration = this.sqlSessionFactory.getConfiguration();

            // step.1 扫描文件
            try {
                this.scanMapperXml();
            } catch (IOException e) {
                logger.error("scanPath扫描包路径配置错误");
                results.put("msg", "scanPath扫描包路径配置错误");
                return results;
            }

            // step.2 判断是否有文件发生了变化
            Map<String, Resource> changedFiles = new HashMap<>(16);
            if (this.isChanged(changedFiles)) {
                XMLMapperBuilder builder;
                for (String key : changedFiles.keySet()) {
                    Resource resource = changedFiles.get(key);
                    // step.2.1 清理
                    this.removeConfig(configuration, key);

                    try{
                        // step.2.2 重新加载
                        builder = new XMLMapperBuilder(resource.getInputStream(), configuration, resource.toString(), configuration.getSqlFragments());
                        builder.parse();
                        logger.info("mapper文件[{}]缓存加载成功", resource.getFilename());
                    } catch (IOException e){
                        logger.error("mapper文件[{}]不存在或内容格式不对", resource.getFilename());
                        continue;
                    }
                }
            }
        } catch (Exception e){
            logger.error("热部署Mybatis映射文件异常！", e);
            results.put("msg", "热部署Mybatis映射文件异常!");
            return results;
        }

        System.out.println("=======检查mybatis映射文件是否更新结束========");

        results.put("msg", "热部署Mybatis映射文件成功!");
        return results;
    }

    private boolean isChanged(Map<String, Resource> changedFiles) throws IOException{
        boolean flag = false;
        for(Resource resource : mapperLocations){
            String resourceName = resource.getFilename();
            long lastFrame = resource.contentLength() + resource.lastModified();

            // 此为新增标识
            boolean addFlag = !fileMapping.containsKey(resourceName);
            if (addFlag){
                // 文件内容帧值
                fileMapping.put(resourceName, Long.valueOf(lastFrame));
                flag = true;
                changedFiles.put(resourceName.split("\\.")[0], resource);
                continue;
            } else{
                // 修改文件:判断文件是否有变化
                Long compareFrame = fileMapping.get(resourceName);
                // 此为修改标识
                boolean modifyFlag = compareFrame != null && compareFrame.longValue() != lastFrame;
                if (modifyFlag){
                    fileMapping.put(resourceName, Long.valueOf(lastFrame));
                    changedFiles.put(resourceName.split("\\.")[0], resource);
                    flag = true;
                }
            }
        }

        return flag;
    }


    /**
     * 清空Configuration中几个重要的缓存
     * @param configuration
     * @throws Exception
     */
    private void removeConfig(Configuration configuration, String keyword) throws Exception{
        Class classConfig = configuration.getClass();
        clearMap(classConfig, configuration, MAPPED_STATEMENTS, keyword);
        clearMap(classConfig, configuration, CACHES, keyword);
        clearMap(classConfig, configuration, RESULT_MAPS, keyword);
        clearMap(classConfig, configuration, PARAMETER_MAPS, keyword);
        clearMap(classConfig, configuration, KEY_GENERATORS, keyword);
        clearMap(classConfig, configuration, SQL_FRAGMENTS, keyword);
        clearSet(classConfig, configuration, LOADED_RESOURCES, keyword);
    }

    private void clearMap(Class<?> classConfig, Configuration configuration, String fieldName, String keyword) throws Exception{
        Field field = classConfig.getDeclaredField(fieldName);
        field.setAccessible(true);
        Map mapConfig = (Map) field.get(configuration);
        Iterator<Map.Entry> iterator = mapConfig.entrySet().iterator();
        while (iterator.hasNext()){
            Map.Entry entry = iterator.next();
            String key = (String) entry.getKey();
            if (key.indexOf(keyword) != -1){
                iterator.remove();
            }
        }
    }

    private void clearSet(Class<?> classConfig, Configuration configuration, String fieldName, String keyword) throws Exception{
        Field field = classConfig.getDeclaredField(fieldName);
        field.setAccessible(true);
        Set setConfig = (Set) field.get(configuration);
        setConfig.removeIf((t) -> ((String) t).indexOf(keyword) != -1);
    }

    /**
     * 扫描xml文件所在的路径
     * @throws IOException
     */

    private void scanMapperXml() throws IOException{
        this.mapperLocations = new PathMatchingResourcePatternResolver().getResources(scanPath);
    }

}
