package problems
import java.io._
object Problem079PickleSerialize {
  @SerialVersionUID(1L) case class Person(name:String, age:Int) extends Serializable
  def roundtrip(p:Person): Person = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos); oos.writeObject(p); oos.close()
    val b = baos.toByteArray
    val ois = new ObjectInputStream(new ByteArrayInputStream(b))
    ois.readObject().asInstanceOf[Person]
  }
}
