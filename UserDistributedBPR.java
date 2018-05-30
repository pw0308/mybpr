import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.mllib.linalg.*;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

/**
 * 用户隐因子因为只跟自己的行为数据有关,所以行为数据分片的同时,也可以把用户隐因子分片
 * 这个的重要意义是不需要多个用户矩阵求平均,求平均降低了收敛速度.
 */
public class UserDistributedBPR {

    static final Integer NUM_LFM =10;
    static final Integer NUM_ITERATIONS=20;
    static final Integer NUM_REDUCERS =10;
    static final Double LAMBDA_REG=0.01;
    static final Double ALPHA=0.1;


    public static void main(String args[]){

        //todo 读取数据
        SparkSession spark = SparkSession.builder().master("local").appName("Simple Application").getOrCreate();
        JavaRDD<String> stringJavaRDD = JavaSparkContext.fromSparkContext(spark.sparkContext())
                .textFile("/Users/zhengpeiwei/Downloads/ml-1m 2/ratings.dat");
        JavaRDD<User_Behavior> user_behaviorJavaRDD=stringJavaRDD.map(new Function<String, User_Behavior>() {
            @Override
            public User_Behavior call(String v1) throws Exception {
                String[] split = v1.split("::");
                return new User_Behavior(split[0],split[1],"1");
            }
        });
        JavaPairRDD<Integer, Integer> ratings = user_behaviorJavaRDD.mapToPair(new PairFunction<User_Behavior, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(User_Behavior user_behavior) throws Exception {
                return new Tuple2<>(Integer.parseInt(user_behavior.getUserId()), Integer.parseInt(user_behavior.getItemId()));
            }
        });


        //todo 求物品数
        final int numItems=(int)ratings.map(new Function<Tuple2<Integer, Integer>, Integer>() {
            @Override
            public Integer call(Tuple2<Integer, Integer> v1) throws Exception {
                return v1._2;
            }
        }).distinct().max(new DummyComparator());


        JavaPairRDD<Integer, Iterable<Integer>> ratingsByUser = ratings.groupByKey().persist(StorageLevel.MEMORY_AND_DISK());
        //<用户id,<其评分们,用户向量>>
        JavaPairRDD<Integer, Tuple2<Iterable<Integer>, Vector>> usersRatingsAndVector = ratingsByUser.mapToPair(new PairFunction<Tuple2<Integer, Iterable<Integer>>, Integer, Tuple2<Iterable<Integer>, Vector>>() {
            @Override
            public Tuple2<Integer, Tuple2<Iterable<Integer>, Vector>> call(Tuple2<Integer, Iterable<Integer>> integerIterableTuple2) throws Exception {
                Random random = new Random();
                double[] values = new double[NUM_LFM];
                for (int pos = 0; pos < NUM_LFM; pos++) {
                    values[pos] = random.nextGaussian();
                }
                return new Tuple2<>(integerIterableTuple2._1, new Tuple2<Iterable<Integer>, Vector>(integerIterableTuple2._2, Vectors.dense(values)));
            }
        });

        //划分一下
        JavaPairRDD<Integer, Tuple2<Iterable<Integer>, Vector>> partitonedUsersRatingsAndVector = usersRatingsAndVector.partitionBy(new HashPartitioner(NUM_REDUCERS)).persist(StorageLevel.MEMORY_AND_DISK());

        //todo 构造物品隐因子矩阵
        Matrix itemMatrix = Matrices.randn(numItems, NUM_LFM, new Random());


        for (int i = 1; i <= NUM_ITERATIONS; i++) {
            final Matrix finalItemMatrix = itemMatrix;
            //每个partiton返回的都是一个<物品矩阵,<我管的用户们id,我管的用户们的向量>Map >
            JavaRDD<Tuple2<Matrix, Map<Integer, Vector>>> result = partitonedUsersRatingsAndVector.mapPartitions(new FlatMapFunction<Iterator<Tuple2<Integer, Tuple2<Iterable<Integer>, Vector>>>, Tuple2<Matrix, Map<Integer, Vector>>>() {
                @Override
                public Iterator<Tuple2<Matrix, Map<Integer, Vector>>> call(Iterator<Tuple2<Integer, Tuple2<Iterable<Integer>, Vector>>> tuple2Iterator) throws Exception {
                    return sampleAndOptimizePartition(tuple2Iterator, finalItemMatrix, numItems);
                }
            });

            //得到物品矩阵的相加结果
            Matrix averagedMatrice = result.map(new Function<Tuple2<Matrix, Map<Integer, Vector>>, Matrix>() {
                @Override
                public Matrix call(Tuple2<Matrix, Map<Integer, Vector>> v1) throws Exception {
                    return v1._1;
                }
            }).reduce(new Function2<Matrix, Matrix, Matrix>() {
                @Override
                public Matrix call(Matrix v1, Matrix v2) throws Exception {
                    Matrix itemM1 = v1, itemM2 = v2;
                    double[] values1 = new double[numItems * NUM_LFM];
                    int count = 0;
                    //注意是按列存的
                    for (int c = 0; c < NUM_LFM; c++) {
                        for (int r = 0; r < numItems; r++) {
                            values1[count] = itemM1.apply(r, c) + itemM2.apply(r, c);
                            count++;
                        }
                    }
                    return Matrices.dense(numItems, NUM_LFM, values1);
                }
            });

            //todo 求物品矩阵的平均
            for(int r=0;r<numItems;r++){
                for(int c=0;c<NUM_LFM;c++){
                    averagedMatrice.update(r,c,averagedMatrice.apply(r,c)/ NUM_REDUCERS);
                }
            }
            itemMatrix = averagedMatrice;

            //把新的用户vectors再join给ratings,得到新的partitonedUsersRatingsAndVector,然后继续迭代
            JavaRDD<Map<Integer, Vector>> map = result.map(new Function<Tuple2<Matrix, Map<Integer, Vector>>, Map<Integer, Vector>>() {
                @Override
                public Map<Integer, Vector> call(Tuple2<Matrix, Map<Integer, Vector>> v1) throws Exception {
                    return v1._2;
                }
            });

            JavaPairRDD<Integer,Vector> userVectorsRDD=map.flatMapToPair(new PairFlatMapFunction<Map<Integer, Vector>, Integer, Vector>() {
                @Override
                public Iterator<Tuple2<Integer, Vector>> call(Map<Integer, Vector> integerVectorMap) throws Exception {
                    ArrayList<Tuple2<Integer,Vector>> res=new ArrayList<>();
                    for(Integer key:integerVectorMap.keySet()){
                        res.add(new Tuple2<Integer, Vector>(key,integerVectorMap.get(key)));
                    }
                    return res.iterator();
                }
            });
            partitonedUsersRatingsAndVector=ratingsByUser.join(userVectorsRDD).cache();
        }

        //todo 最后构建用户隐因子矩阵(关键你的用户矩阵按userId编号来)

    }



    //对一个partition的数据进行采样和优化

    /**
     *
     * @param userRatingsFeatures 是一个<用户,<看过物品集,其向量的>>iterator
     * @param itemMatrix
     * @param numItems
     * @return
     */
    private static Iterator<Tuple2<Matrix, Map<Integer,Vector>>> sampleAndOptimizePartition(Iterator<Tuple2<Integer, Tuple2<Iterable<Integer>, Vector>>> userRatingsFeatures, Matrix itemMatrix, int numItems) {

        //得到一个<Userid,DenseVector>Map
        Map<Integer,Vector> userIdAndItsVector=new HashMap();
        for (Iterator<Tuple2<Integer, Tuple2<Iterable<Integer>, Vector>>> it = userRatingsFeatures; it.hasNext();) {
            Tuple2<Integer, Tuple2<Iterable<Integer>, Vector>> one = it.next();
            userIdAndItsVector.put(one._1,one._2._2);
        }


        //采样
        //<用户id,其看过的物品>
        Map<Integer,Set<Integer>> observed=new HashMap<>();
        //使用observed记录一个用户所看过的物品
        for (Iterator<Tuple2<Integer, Tuple2<Iterable<Integer>, Vector>>> it = userRatingsFeatures; it.hasNext();) {
            //对于一个<用户,<看过物品集,其向量的>>
            Tuple2<Integer, Tuple2<Iterable<Integer>, Vector>> one = it.next();
            if(!observed.containsKey(one._1)){
                HashSet<Integer> items = new HashSet<>();
                for(Integer i:one._2._1){
                    items.add(i);
                }
                observed.put(one._1,items);
            }
            else{
                for(Integer i:one._2._1){
                    observed.get(one._1).add(i);
                }
            }
        }
        //可用于生成1-numItems的随机数
        Random randomItem=new Random();
        Random randomUser=new Random();
        //observed里只有这个partition的用户,可以用来用户抽样
        Integer[] keys = observed.keySet().toArray(new Integer[0]);
        //把(u,i,0)=>(u,i,j)
        ArrayList<Tuple2<Integer,Tuple2<Integer,Integer>>> samples=new ArrayList<>();
        //生成100*User个sample
        for (int sampleCount = 0, smax = keys.length * 100; sampleCount < smax; sampleCount++) {
            //随机抽样s
            int userIdx, posItemIdx, negItemIdx;
            while (true) {
                //选取一个用户
                userIdx = keys[randomUser.nextInt(keys.length)];
                //这个用户的看过物品集
                Set<Integer> itemSet = observed.get(userIdx);
                if (itemSet.size() == 0 || itemSet.size() == numItems)
                    continue;
                //构造成List好随机抽样
                List<Integer> itemList=new ArrayList<>(itemSet);

                //从itemSet中随机抽取一个看过的物品
                //从用户行为列的List中抽取一个行为过的物品
                posItemIdx = itemList.get(randomItem.nextInt(itemList.size()));
                //随机抽取直到抽取到一个用户行为列之中没有的物品
                do {
                    negItemIdx = randomItem.nextInt(numItems)+1;
                } while (itemSet.contains(negItemIdx));
                samples.add(new Tuple2<>(userIdx,new Tuple2<Integer, Integer>(posItemIdx,negItemIdx)));
                break;
            }
        }
        //训练
        for(Tuple2<Integer,Tuple2<Integer,Integer>> sample:samples){
            gradientSinglePoint(sample._1, sample._2._1, sample._2._2, itemMatrix,userIdAndItsVector);
        }

        ArrayList<Tuple2<Matrix,Map<Integer,Vector>>> result=new ArrayList<>();
        result.add(new Tuple2<Matrix, Map<Integer,Vector>>(itemMatrix,userIdAndItsVector));
        return result.iterator();
    }


    private static void gradientSinglePoint(Integer userId, Integer prodPos, Integer prodNeg, Matrix itemMatrix, Map<Integer, Vector> userIdAndItsVector) {

        Double xui=0.0,xuj=0.0,x_uij;
        Vector userVector = userIdAndItsVector.get(userId);
        for(int k=0;k<NUM_LFM;k++){
            xui+=(userVector.apply(k)*itemMatrix.apply(prodPos-1,k));
            xuj+=(userVector.apply(k)*itemMatrix.apply(prodNeg-1,k));
        }
        x_uij=xui-xuj;Double deri=Math.exp(-x_uij)/(1+Math.exp(-x_uij));

        double[] newUserVector=new double[NUM_LFM];

        //更新各个向量
        for(int k=0;k<NUM_LFM;k++){
            //必须先取出来,不然若你更新了user,然而item的更新要用到user,则用到的就不是原来的值了
            double userFactorValue = userVector.apply(k);
            //获取正物品的物品向量中一个factor现有值
            double posItemFactorValue = itemMatrix.apply(prodPos-1, k);
            //获取负物品的物品向量中一个factor现有值
            double negItemFactorValue = itemMatrix.apply(prodNeg-1, k);

            double addPos=ALPHA*(deri*userFactorValue-LAMBDA_REG*posItemFactorValue);
            itemMatrix.update(prodPos-1,k,posItemFactorValue+addPos);
            double addNeg=ALPHA*(deri*(-userFactorValue)-LAMBDA_REG*negItemFactorValue);
            itemMatrix.update(prodNeg-1,k,negItemFactorValue+addNeg);

            //更新用户vector,因为Vector没提供更新方法,所以有点麻烦,每一维都得重做
            newUserVector[k]=userFactorValue+ALPHA*(deri*(posItemFactorValue-negItemFactorValue)-LAMBDA_REG*userFactorValue);
        }
        userIdAndItsVector.put(userId,Vectors.dense(newUserVector));

    }



    /**
     * 预测分数的函数
     * @param userMatrix
     * @param itemMatrix
     */
    private static double predict(Matrix userMatrix, Matrix itemMatrix, int user, int item) {
        double score=0.0;
        int k=userMatrix.numCols();
        for(int i=0;i<k;i++){
            score+=(userMatrix.apply(user-1,i)*itemMatrix.apply(item-1,i));
        }
        return score;
    }

}
