package com.gsta.bigdata.utils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * xml parse tools
 * @author tianxq
 *
 */
public class XmlTools {
	private static String encoding = "utf-8";

	public static void setDefaultEncoding(String encoding) {
		XmlTools.encoding = encoding;
	}

	public static Document newDocument(String root)
			throws ParserConfigurationException {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		factory.setNamespaceAware(true);
		
		DocumentBuilder db = factory.newDocumentBuilder();
		Document doc = db.newDocument();
		Element element = doc.createElement(root);
		doc.appendChild(element);
		
		return doc;
	}

	public static Document newDocument() throws ParserConfigurationException {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		factory.setNamespaceAware(true);
		
		DocumentBuilder db = factory.newDocumentBuilder();
		Document doc = db.newDocument();
		
		return doc;
	}

	public static Document loadFromInputStream(InputStream inputStream)
			throws ParserConfigurationException, SAXException, IOException {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		factory.setNamespaceAware(true);
		
		DocumentBuilder db = factory.newDocumentBuilder();
		return db.parse(inputStream);
	}

	public static Document loadFromFile(File file)
			throws ParserConfigurationException, SAXException, IOException {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		factory.setNamespaceAware(true);
		
		DocumentBuilder db = factory.newDocumentBuilder();
		return db.parse(file);
	}

	public static Document loadFromURI(String uri)
			throws ParserConfigurationException, SAXException, IOException {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		factory.setNamespaceAware(true);
		
		DocumentBuilder db = factory.newDocumentBuilder();
		return db.parse(uri);
	}

	public static Document loadFromContent(String content)
			throws ParserConfigurationException, SAXException, IOException {
		return loadFromInputStream(new ByteArrayInputStream(content.getBytes()));
	}

	public static void saveToOutputStream(Document doc, OutputStream out)
			throws TransformerException, IOException {
		saveToOutputStream(doc, out, true);
	}

	public static void saveToOutputStream(Node node, OutputStream out,
			boolean out_head) throws TransformerException {
		TransformerFactory factory = TransformerFactory.newInstance();
		Transformer transformer = factory.newTransformer();
		
		if (!out_head) {
			transformer.setOutputProperty("omit-xml-declaration", "yes");
		} else {
			transformer.setOutputProperty("omit-xml-declaration", "no");
		}
		
		transformer.setOutputProperty("encoding", encoding);
		Source source = new DOMSource(node);
		Result result = new StreamResult(out);
		transformer.transform(source, result);
	}

	public static String node2String(Node node) throws TransformerException {
		TransformerFactory factory = TransformerFactory.newInstance();
		
		Transformer transformer = factory.newTransformer();
		transformer.setOutputProperty("omit-xml-declaration", "yes");
		transformer.setOutputProperty("encoding", encoding);
		
		Source source = new DOMSource(node);
		StringWriter writer = new StringWriter(1024);
		Result result = new StreamResult(writer);
		
		transformer.transform(source, result);
		return writer.toString();
	}

	public static void xslt(Document data, Document xsl, Result result,
			boolean outHead) throws TransformerException {
		TransformerFactory factory = TransformerFactory.newInstance();
		Templates templates = factory.newTemplates(new DOMSource(xsl));
		Transformer transformer = templates.newTransformer();
		
		if (!outHead) {
			transformer.setOutputProperty("omit-xml-declaration", "yes");
		} else {
			transformer.setOutputProperty("omit-xml-declaration", "no");
		}
		
		transformer.setOutputProperty("encoding", encoding);
		transformer.transform(new DOMSource(data), result);
	}

	public static void xslt(Document data, Document xsl, OutputStream out,
			boolean outHead) throws TransformerException {
		xslt(data, xsl, new StreamResult(out), outHead);
	}

	public static void xslt(Document data, Document xsl, OutputStream out)
			throws TransformerException {
		xslt(data, xsl, new StreamResult(out), true);
	}

	public static Document xslt(Document data, Document xsl)
			throws TransformerException, ParserConfigurationException {
		Document out = XmlTools.newDocument();
		xslt(data, xsl, new DOMResult(out), true);
		
		return out;
	}

	public static void Clone(Element root, Element src) {
		Document doc = root.getOwnerDocument();
		NodeList nodes = src.getChildNodes();
		for (int i = 0; i < nodes.getLength(); i++) {
			Node node = nodes.item(i);
			int nodeType = node.getNodeType();
			switch (nodeType) {
			case Node.TEXT_NODE:
				root.appendChild(doc.createTextNode(node.getNodeValue()));
				break;
			case Node.ELEMENT_NODE:
				Element _element = doc.createElement(node.getNodeName());
				// clone attribute
				NamedNodeMap attrs = node.getAttributes();
				for (int j = 0; j < attrs.getLength(); j++) {
					Node attr = attrs.item(j);
					_element.setAttribute(attr.getNodeName(),
							attr.getNodeValue());
				}
				
				// clone children
				Clone(_element, (Element) node);
				root.appendChild(_element);
				break;
			}
		}
	}

	/**
	 * get element first child by tag name
	 * @param parent
	 * @param tagName
	 * @return
	 */
	public static Element getFirstChildByTagName(Element parent,
			String tagName) {
		NodeList nodeList = parent.getChildNodes();
		
		for (int i = 0; i < nodeList.getLength(); i++) {
			Node node = nodeList.item(i);
			if (node.getNodeType() != Node.ELEMENT_NODE)
				continue;
			if (node.getNodeName().equals(tagName))
				return (Element) node;
		}
		
		return null;
	}
	
	public static List<Element> getChildrenByTagName(Element parent,String tagName) {
		List<Element> children = new ArrayList<Element>();
		
		NodeList nodeList = parent.getChildNodes();
		for (int i = 0; i < nodeList.getLength(); i++) {
			Node node = nodeList.item(i);
			if (node.getNodeType() == Node.ELEMENT_NODE
					&& node.getNodeName().equals(tagName)) {
				children.add((Element) node);
			}
		}

		return children;
	}

	/**
	 * <etl>
	 * 	 <process id="ex0">
	 * 		<name>abc</name>
	 * 	 </process>
	 * </etl>
	 * @param element  etl element,path ex:"/etl/process[@id='ex0']/name"
	 * @return  ex:abc
	 * @throws XPathExpressionException 
	 */
	public static String getNodeValue(Element element) throws XPathExpressionException{
		XPath xpath = XPathFactory.newInstance().newXPath();
		return (String) xpath.evaluate("text()", element,XPathConstants.STRING);
	}
	
	/**
	 *
	 * <etl>
	 * 	 <process id="ex0" desc="xyz">
	 * 		<name>abc</name>
	 * 	 </process>
	 * </etl>
	 * @param element ,ex:path=/etl/process[@id='ex0']
	 * @param attrName ,ex:desc
	 * @return ex:xyz
	 * @throws XPathExpressionException 
	 */
	public static String getNodeAttr(Element element,String attrName) throws XPathExpressionException{
		XPath xpath = XPathFactory.newInstance().newXPath();
		return  (String) xpath.evaluate("@" + attrName,element,XPathConstants.STRING);
	}
	
	public static Map<String, String> getNodeAttrs(Element element) {
		Map<String, String> ret = new HashMap<String, String>();
		NamedNodeMap attrs = element.getAttributes();
		for (int i = 0; i < attrs.getLength(); i++) {
			Node node = attrs.item(i);
			ret.put(node.getNodeName(),node.getNodeValue());
		}
		
		return ret;
	}
	
	/**
	 * 
	 * <etl>
	 * 	 <process id="ex0" desc="xyz">
	 * 		<name>abc</name>
	 * 	 </process>
	 * </etl>
	 * @param document
	 * @param path ex:/etl/process[@id='ex0']
	 * @return ex:process node
	 * @throws XPathExpressionException
	 */
	public static Node getNodeByPath(Document document,String path) throws XPathExpressionException{
		XPath xpath = XPathFactory.newInstance().newXPath();
		return (Node) xpath.evaluate(path, document,XPathConstants.NODE);
	}
	
	/**
	 * <etl>
	 * 	 <process id="ex0" desc="xyz">
	 * 		<name>abc</name>
	 * 	 </process>
	 * 	 <process id="ex1" desc="xyz">
	 * 		<name>abc</name>
	 * 	 </process>
	 * </etl>
	 * @param document
	 * @param path ex:"/etl/process"
	 * @return ex:process id=ex0 and process id=ex1
	 * @throws XPathExpressionException
	 */
	public static NodeList getNodeListByPath(Document document,String path) throws XPathExpressionException{
		XPath xpath = XPathFactory.newInstance().newXPath();
		return (NodeList) xpath.evaluate(path, document,XPathConstants.NODESET);
	}
	
	public static NodeList getNodeListByPath(Element element,String path) throws XPathExpressionException{
		XPath xpath = XPathFactory.newInstance().newXPath();
		return (NodeList) xpath.evaluate(path, element,XPathConstants.NODESET);
	}
}
